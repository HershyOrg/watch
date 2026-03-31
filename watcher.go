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
type Watcher struct {
	config  WatcherConfig
	manager *manager.Manager

	// State
	isRunning atomic.Bool // watcher자체가 실행중인지의 값. (Run/ Stop)

	// WatchMachine Registry
	machineRegistry *shared.SafeMap[string, *wm.WatchMachine]

	// Lifecycle
	rootCtx    context.Context
	rootCancel context.CancelFunc

	// API Server
	apiServer *WatcherAPIServer
}

// NewWatcher creates a new Watcher with the given configuration.
// The Manager is created later when Manage is called.
//
// parentCtx (optional): Parent context for lifecycle management.
//   - If provided: Watcher automatically stops when context is cancelled
//   - If nil: Uses context.Background()
//   - Auto-stop has 5-minute timeout, then forces shutdown
func NewWatcher(config WatcherConfig, parentCtx context.Context) *Watcher {
	if config.DefaultTimeout == 0 {
		config = DefaultWatcherConfig()
	}

	// Use parent context if provided, otherwise use Background
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	// Create independent context for Watcher
	rootCtx, cancel := context.WithCancel(context.Background())

	w := &Watcher{
		config:          config,
		manager:         nil, // Manager created in Manage()
		machineRegistry: shared.NewSafeMap[string, *wm.WatchMachine](),
		rootCtx:         rootCtx,
		rootCancel:      cancel,
	}

	// Auto-shutdown goroutine: monitors parent context
	go func() {
		<-parentCtx.Done()

		if !w.isRunning.Load() {
			return // Already stopped
		}

		fmt.Println("[Watcher] Parent context cancelled, stopping...")

		// Call Stop() with 5-minute timeout
		stopDone := make(chan error, 1)
		go func() {
			stopDone <- w.Stop()
		}()

		select {
		case err := <-stopDone:
			if err != nil {
				fmt.Printf("[Watcher] Stop error: %v\n", err)
			}
		case <-time.After(5 * time.Minute):
			fmt.Println("[Watcher] Stop timeout (5 min), forcing shutdown...")
			w.forceShutdown()
		}
	}()

	return w
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

// Start begins the Watcher's execution.
func (w *Watcher) Start() error {
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
		return fmt.Errorf("failed to start API server: %w", err)
	}
	w.apiServer = apiServer

	// Send an initial empty UserEvent to trigger first execution
	w.manager.SendUserEvent(nil)

	return nil
}

// Stop gracefully stops the Watcher.
func (w *Watcher) Stop() error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}

	// Check if Manager is already in a terminal state
	currentState := w.manager.GetControlState()
	if currentState.IsTerminal() {
		// Already stopped - just clean up Watcher resources
		if w.apiServer != nil {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			w.apiServer.Shutdown(shutdownCtx)
			w.apiServer = nil
		}
		w.stopAllWatches()
		w.rootCancel()
		w.isRunning.Store(false)
		return nil
	}

	// Send Stop control event
	w.manager.RequestStop("user requested stop")

	// Wait for cleanup completion and terminal state using polling
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(300 * time.Second)

	for {
		select {
		case <-ticker.C:
			cs := w.manager.GetControlState()
			if cs.IsTerminal() && w.manager.IsCleanupCompleted() {
				goto StopCompleted
			}
		case <-timeout:
			return fmt.Errorf("stop timeout: cleanup and state transition not completed within 60 seconds")
		}
	}

StopCompleted:
	// Shutdown API server
	if w.apiServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := w.apiServer.Shutdown(shutdownCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				fmt.Println("[Watcher] API server shutdown timeout (5s), forcing close...")
				if closeErr := w.apiServer.Close(); closeErr != nil {
					fmt.Printf("[Watcher] API server force close error: %v\n", closeErr)
				} else {
					fmt.Println("[Watcher] API server force closed successfully")
				}
			} else {
				fmt.Printf("[Watcher] API server shutdown error: %v\n", err)
			}
		} else {
			fmt.Println("[Watcher] API server stopped gracefully")
		}
		w.apiServer = nil
	}

	// Finalize Watcher shutdown
	w.stopAllWatches()
	w.rootCancel()
	w.isRunning.Store(false)

	return nil
}

// forceShutdown forcefully terminates the Watcher (last resort).
func (w *Watcher) forceShutdown() {
	fmt.Println("[Watcher] Force shutdown initiated...")

	w.rootCancel()
	w.stopAllWatches()

	if w.apiServer != nil {
		if err := w.apiServer.Close(); err != nil {
			fmt.Printf("[Watcher] Force close API server error: %v\n", err)
		}
	}

	w.isRunning.Store(false)
	fmt.Println("[Watcher] Force shutdown complete")
}

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

// StopManager stops the Manager (enters Stopped state).
func (w *Watcher) StopManager() error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}

	currentState := w.manager.GetControlState()
	if currentState.IsTerminal() {
		return nil // Already stopped
	}

	w.manager.RequestStop("user requested manager stop")

	return w.waitForTerminalState(60 * time.Second)
}

// RunManager restarts the Manager from a terminal state.
func (w *Watcher) RunManager() error {
	if !w.isRunning.Load() {
		return fmt.Errorf("watcher not running")
	}

	currentState := w.manager.GetControlState()
	if !currentState.IsTerminal() {
		return fmt.Errorf("can only run from terminal states, current: %s", currentState)
	}

	// Send RunRequested control event with NeedInit
	w.manager.RequestRun("user requested manager restart", true)

	// Wait for state transition
	time.Sleep(100 * time.Millisecond)

	// Trigger first execution with empty message
	w.manager.SendUserEvent(nil)

	// Wait for Idle state
	return w.waitForState(ControlIdle, 60*time.Second)
}

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
