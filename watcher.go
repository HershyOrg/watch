package watch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// Watcher is the core reactive framework engine.
// It manages reactive state through Watch, executes managed functions,
// and provides fault tolerance through supervision.
//
// Watcher는 3가지 범위를 제어하며, 각각 별도 메서드로 분리:
//   - Self: StartSelf(), StopAll()
//   - Manager: RunMgr(), StopMgr(), KillMgr()
//   - WatchMachine: StartWm(), StopWm(), KillWm()
type Watcher struct {
	config  WatcherConfig
	manager *manager.Manager

	// State
	isRunning atomic.Bool // watcher자체가 실행중인지의 값. (StartSelf / StopAll)

	// WatchMachine Registry
	machineRegistry *shared.SafeMap[string, *wm.WatchMachine]

	// API Server
	apiServer *WatcherAPIServer
}

// NewWatcher creates a new Watcher with the given configuration.
// The Manager is created later when Manage is called.
// Watcher는 자체 내부 context를 생성한다 (외부 ctx를 받지 않음).
func NewWatcher(config WatcherConfig) *Watcher {
	if config.DefaultTimeout == 0 {
		config = DefaultWatcherConfig()
	}

	return &Watcher{
		config:          config,
		manager:         nil, // Manager created in Manage()
		machineRegistry: shared.NewSafeMap[string, *wm.WatchMachine](),
	}
}

// Manage registers a function to be managed by the Watcher.
func (w *Watcher) Manage(fn manager.ManagedFunc, name string, envVars map[string]string) *manager.CleanupBuilder {
	if w.isRunning.Load() {
		panic("cannot call Manage after Watcher is already running")
	}

	wrappedFn := func(msg *Message, ctx ManageContext) error {
		return fn(msg, ctx)
	}

	w.manager = manager.NewManager(
		w.config,
		name,
		wrappedFn,
		nil, // Cleaner set via CleanupBuilder
		envVars,
	)

	w.manager.SetMachineRegistry(w)

	return manager.NewCleanupBuilder(w.manager)
}

// --- Self 범위 (Start 기반) ---

// StartSelf begins the Watcher's own resources (API server).
// isRunning을 true로 설정한다. Manager 실행은 별도로 RunMgr()를 호출해야 한다.
func (w *Watcher) StartSelf() error {
	if !w.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("watcher already running")
	}

	if w.manager == nil {
		w.isRunning.Store(false)
		return fmt.Errorf("no managed function registered")
	}

	// Start API server (non-blocking)
	apiServer, err := w.StartAPIServer()
	if err != nil {
		w.isRunning.Store(false)
		return fmt.Errorf("failed to start API server: %w", err)
	}
	w.apiServer = apiServer

	return nil
}

// StopAll gracefully stops everything: Manager → WatchMachines → Self.
// "자기 자신 종료 = 전체 종료"라는 의미론.
func (w *Watcher) StopAll() error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}

	// 1. Manager 정지
	if w.manager != nil {
		currentState := w.manager.GetControlState()
		if !currentState.IsTerminal() {
			w.manager.RequestStop("StopAll requested")

			// 터미널 상태 + cleanup 완료 대기
			ticker := time.NewTicker(500 * time.Millisecond)
			timeout := time.After(300 * time.Second)

		waitLoop:
			for {
				select {
				case <-ticker.C:
					cs := w.manager.GetControlState()
					if cs.IsTerminal() && w.manager.IsCleanupCompleted() {
						break waitLoop
					}
				case <-timeout:
					fmt.Println("[Watcher] StopAll: Manager stop timeout, forcing kill...")
					w.manager.RequestKill("StopAll timeout")
					time.Sleep(1 * time.Second)
					break waitLoop
				}
			}
			ticker.Stop()
		}

		// 2. Manager 이벤트 루프 종료
		w.manager.Close()
	}

	// 3. 모든 WatchMachine 정지
	w.stopAllWatches()

	// 4. API 서버 Shutdown
	if w.apiServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := w.apiServer.Shutdown(shutdownCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				fmt.Println("[Watcher] API server shutdown timeout (5s), forcing close...")
				if closeErr := w.apiServer.Close(); closeErr != nil {
					fmt.Printf("[Watcher] API server force close error: %v\n", closeErr)
				}
			} else {
				fmt.Printf("[Watcher] API server shutdown error: %v\n", err)
			}
		}
		w.apiServer = nil
	}

	// 5. Watcher 자체 종료
	w.isRunning.Store(false)

	return nil
}

// --- Syntactic Sugar ---

// StartAndRun is a convenience method: StartSelf() + RunMgr().
// Watcher를 시작하고 Manager의 ManagedFunc를 초기 실행한다.
func (w *Watcher) StartAndRun() error {
	if err := w.StartSelf(); err != nil {
		return err
	}
	return w.RunMgr()
}

// --- Manager 범위 (Run 기반) ---

// RunMgr executes the Manager's ManagedFunc.
// 초기 실행과 재실행(터미널 상태에서) 모두 이 메서드 하나로 처리.
func (w *Watcher) RunMgr() error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}
	if w.manager == nil {
		return fmt.Errorf("no managed function registered")
	}

	currentState := w.manager.GetControlState()

	if currentState.IsTerminal() {
		// 재실행: RequestRun(needInit=true) + Idle 상태 대기
		w.manager.RequestRun("run requested", true)
		return w.waitForState(ControlIdle, 60*time.Second)
	}

	if currentState == ControlIdle {
		// 초기 실행
		w.manager.RequestRun("watcher started", true)
		return nil
	}

	return fmt.Errorf("manager is already running (current state: %s)", currentState)
}

// StopMgr sends a stop event to the Manager and waits for terminal state.
func (w *Watcher) StopMgr() error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}
	if w.manager == nil {
		return fmt.Errorf("no managed function registered")
	}

	currentState := w.manager.GetControlState()
	if currentState.IsTerminal() {
		return nil // Already stopped
	}

	w.manager.RequestStop("user requested manager stop")

	return w.waitForTerminalState(60 * time.Second)
}

// KillMgr forcefully terminates the Manager.
func (w *Watcher) KillMgr() error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}
	if w.manager == nil {
		return fmt.Errorf("no managed function registered")
	}

	currentState := w.manager.GetControlState()
	if currentState.IsTerminal() {
		return nil
	}

	w.manager.RequestKill("user requested manager kill")

	return w.waitForTerminalState(30 * time.Second)
}

// --- WatchMachine 범위 (Start 기반) ---

// StartWm starts a specific WatchMachine by varName.
func (w *Watcher) StartWm(varName string) error {
	machine, ok := w.machineRegistry.Load(varName)
	if !ok {
		return fmt.Errorf("watchMachine not found: %s", varName)
	}
	machine.Start()
	return nil
}

// StopWm stops a specific WatchMachine by varName.
func (w *Watcher) StopWm(varName string) error {
	machine, ok := w.machineRegistry.Load(varName)
	if !ok {
		return fmt.Errorf("watchMachine not found: %s", varName)
	}
	machine.Stop()
	return nil
}

// KillWm forcefully terminates a specific WatchMachine by varName.
func (w *Watcher) KillWm(varName string) error {
	machine, ok := w.machineRegistry.Load(varName)
	if !ok {
		return fmt.Errorf("watchMachine not found: %s", varName)
	}
	machine.Kill()
	return nil
}

// --- 메시지 전송 ---

// SendMessage sends a user message to the managed function.
func (w *Watcher) SendMessage(content string) error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}

	msg := &Message{
		Content:    content,
		ReceivedAt: time.Now(),
	}

	w.manager.SendUserEvent(msg)

	return nil
}

// --- 상태 조회 ---

// GetState returns the current ControlState.
func (w *Watcher) GetState() ControlState {
	return w.manager.GetControlState()
}

// GetLogger returns the Watcher's logger for inspection.
func (w *Watcher) GetLogger() *manager.Logger {
	return w.manager.GetLogger()
}

// GetManager returns the internal Manager for testing purposes.
func (w *Watcher) GetManager() *manager.Manager {
	return w.manager
}

// IsRunning returns whether the Watcher is currently running.
func (w *Watcher) IsRunning() bool {
	return w.isRunning.Load()
}

// --- private helpers ---

// waitForState waits for Manager to reach the target ControlState.
func (w *Watcher) waitForState(targetState ControlState, timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			currentState := w.manager.GetControlState()
			if currentState == targetState {
				return nil
			}
			if targetState != ControlCrashed && currentState == ControlCrashed {
				return fmt.Errorf("manager crashed while waiting for state %s", targetState)
			}
			if targetState != ControlKilled && currentState == ControlKilled {
				return fmt.Errorf("manager killed while waiting for state %s", targetState)
			}
		case <-deadline:
			currentState := w.manager.GetControlState()
			return fmt.Errorf("timeout waiting for state %s (current: %s)", targetState, currentState)
		}
	}
}

// waitForTerminalState waits for Manager to reach any terminal ControlState.
func (w *Watcher) waitForTerminalState(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			if w.manager.GetControlState().IsTerminal() {
				return nil
			}
		case <-deadline:
			currentState := w.manager.GetControlState()
			return fmt.Errorf("timeout waiting for terminal state (current: %s)", currentState)
		}
	}
}

// stopAllWatches stops all WatchMachines with 1 minute timeout.
func (w *Watcher) stopAllWatches() {
	done := make(chan struct{})
	go func() {
		w.machineRegistry.Range(func(varName string, machine *wm.WatchMachine) bool {
			machine.Stop()
			return true
		})
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("[Watcher] All watches stopped successfully")
	case <-time.After(1 * time.Minute):
		fmt.Println("[Watcher] Warning: Some watches did not stop within 1min timeout")
	}
}

// --- wm.MachineRegistry interface implementation ---

// RegisterWatchMachine registers a WatchMachine to the Watcher's registry.
func (w *Watcher) RegisterWatchMachine(managerName string, watchMachine *wm.WatchMachine) error {
	if watchMachine == nil {
		return fmt.Errorf("watchMachine is nil")
	}
	w.machineRegistry.Store(watchMachine.VarName, watchMachine)
	return nil
}

// GetWatchMachine returns the WatchMachine for the given varName.
func (w *Watcher) GetWatchMachine(varName string) (*wm.WatchMachine, bool) {
	return w.machineRegistry.Load(varName)
}

// GetAllWatchMachines returns all registered WatchMachines.
func (w *Watcher) GetAllWatchMachines() []*wm.WatchMachine {
	return w.machineRegistry.Values()
}

// UnregisterWatchMachine removes a WatchMachine from the registry.
func (w *Watcher) UnregisterWatchMachine(varName string) error {
	if _, ok := w.machineRegistry.Load(varName); !ok {
		return fmt.Errorf("watchMachine not found: %s", varName)
	}
	w.machineRegistry.Delete(varName)
	return nil
}
