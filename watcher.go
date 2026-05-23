package watch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

var (
	ErrNoManagedFunction = errors.New("no managed function registered")
	ErrWatcherNotRunning = errors.New("watcher not running")
	ErrInvalidConfig     = errors.New("invalid watcher config")
)

// Watcher is the core reactive framework engine.
// It manages reactive state through Watch, executes managed functions,
// and provides fault tolerance through supervision.
type Watcher struct {
	config  WatcherConfig
	manager *manager.Manager

	lifecycleMu sync.Mutex
	isRunning   atomic.Bool

	machineRegistry *shared.SafeMap[string, *wm.WatchMachine]
	apiServer       *watcherAPIServer
}

// NewWatcher creates a new Watcher with the given configuration.
// The Manager is created later when Manage is called.
func NewWatcher(config WatcherConfig) (*Watcher, error) {
	if err := validateWatcherConfig(config); err != nil {
		return nil, err
	}

	return &Watcher{
		config:          config,
		manager:         nil,
		machineRegistry: shared.NewSafeMap[string, *wm.WatchMachine](),
	}, nil
}

// Manage registers a function to be managed by the Watcher.
func (w *Watcher) Manage(fn manager.ManagedFunc, name string, envVars map[string]any) *manager.CleanupBuilder {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()

	if w.isRunning.Load() {
		panic("cannot call Manage after Watcher is already running")
	}

	wrappedFn := func(msg *Message, ctx ManageContext) (shared.ControlSignal, error) {
		return fn(msg, ctx)
	}

	w.manager = manager.NewManager(
		w.config,
		name,
		wrappedFn,
		nil,
		envVars,
	)

	w.manager.SetMachineRegistry(w)

	return manager.NewCleanupBuilder(w.manager)
}

// Start starts the Watcher's resources and runs the managed function once.
func (w *Watcher) Start() error {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()

	if err := w.startSelf(); err != nil {
		return err
	}
	if err := w.runManager(); err != nil {
		_ = w.stopLocked(context.Background())
		return err
	}
	return nil
}

// Run starts the Watcher and blocks until the manager reaches a terminal state
// or ctx is cancelled. On ctx cancellation, Run performs graceful shutdown.
func (w *Watcher) Run(ctx context.Context) (ControlState, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := w.Start(); err != nil {
		return w.currentState(), err
	}

	state, err := w.waitForTerminalOrContext(ctx)
	if err == nil {
		if stopErr := w.Stop(context.Background()); stopErr != nil {
			return state, stopErr
		}
		return state, nil
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return state, err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), w.config.ShutdownTimeout)
	defer cancel()

	if stopErr := w.Stop(shutdownCtx); stopErr != nil {
		return w.currentState(), stopErr
	}

	return w.currentState(), nil
}

func validateWatcherConfig(config WatcherConfig) error {
	var errs []error

	if config.ServerPort <= 0 || config.ServerPort > 65535 {
		errs = append(errs, fmt.Errorf("ServerPort must be in range 1..65535 (got %d)", config.ServerPort))
	}
	if config.DefaultTimeout <= 0 {
		errs = append(errs, fmt.Errorf("DefaultTimeout must be positive (got %s)", config.DefaultTimeout))
	}
	if config.HealthCheckInterval <= 0 {
		errs = append(errs, fmt.Errorf("HealthCheckInterval must be positive (got %s)", config.HealthCheckInterval))
	}
	if config.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("ShutdownTimeout must be positive (got %s)", config.ShutdownTimeout))
	}
	if config.CleanupTimeout <= 0 {
		errs = append(errs, fmt.Errorf("CleanupTimeout must be positive (got %s)", config.CleanupTimeout))
	}
	if config.WatchMachineHistoryMaxLen <= 0 {
		errs = append(errs, fmt.Errorf("WatchMachineHistoryMaxLen must be positive (got %d)", config.WatchMachineHistoryMaxLen))
	}
	if config.WatchMachineHistoryMaxDur <= 0 {
		errs = append(errs, fmt.Errorf("WatchMachineHistoryMaxDur must be positive (got %s)", config.WatchMachineHistoryMaxDur))
	}
	if config.MaxLogEntries <= 0 {
		errs = append(errs, fmt.Errorf("MaxLogEntries must be positive (got %d)", config.MaxLogEntries))
	}
	if config.MaxMemoEntries <= 0 {
		errs = append(errs, fmt.Errorf("MaxMemoEntries must be positive (got %d)", config.MaxMemoEntries))
	}
	if config.SignalChanCapacity <= 0 {
		errs = append(errs, fmt.Errorf("SignalChanCapacity must be positive (got %d)", config.SignalChanCapacity))
	}

	policy := config.RecoveryPolicy
	if policy.MinConsecutiveFailures < 1 {
		errs = append(errs, fmt.Errorf("RecoveryPolicy.MinConsecutiveFailures must be >= 1 (got %d)", policy.MinConsecutiveFailures))
	}
	if policy.MaxConsecutiveFailures < policy.MinConsecutiveFailures {
		errs = append(errs, fmt.Errorf(
			"RecoveryPolicy.MaxConsecutiveFailures must be >= MinConsecutiveFailures (got max=%d min=%d)",
			policy.MaxConsecutiveFailures,
			policy.MinConsecutiveFailures,
		))
	}
	if policy.BaseRetryDelay <= 0 {
		errs = append(errs, fmt.Errorf("RecoveryPolicy.BaseRetryDelay must be positive (got %s)", policy.BaseRetryDelay))
	}
	if policy.MaxRetryDelay <= 0 {
		errs = append(errs, fmt.Errorf("RecoveryPolicy.MaxRetryDelay must be positive (got %s)", policy.MaxRetryDelay))
	}
	if policy.MaxRetryDelay < policy.BaseRetryDelay {
		errs = append(errs, fmt.Errorf(
			"RecoveryPolicy.MaxRetryDelay must be >= BaseRetryDelay (got max=%s base=%s)",
			policy.MaxRetryDelay,
			policy.BaseRetryDelay,
		))
	}
	if len(policy.LightweightRetryDelays) == 0 {
		errs = append(errs, fmt.Errorf("RecoveryPolicy.LightweightRetryDelays must be non-empty"))
	}
	for i, delay := range policy.LightweightRetryDelays {
		if delay <= 0 {
			errs = append(errs, fmt.Errorf("RecoveryPolicy.LightweightRetryDelays[%d] must be positive (got %s)", i, delay))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return nil
}

func (w *Watcher) startSelf() error {
	if !w.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("watcher already running")
	}

	if w.manager == nil {
		w.isRunning.Store(false)
		return ErrNoManagedFunction
	}

	apiServer, err := w.startAPIServer()
	if err != nil {
		w.isRunning.Store(false)
		w.apiServer = nil
		return fmt.Errorf("failed to start API server: %w", err)
	}
	w.apiServer = apiServer

	return nil
}

// Stop gracefully stops everything: Manager -> WatchMachines -> API server.
func (w *Watcher) Stop(ctx context.Context) error {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()

	return w.stopLocked(ctx)
}

func (w *Watcher) stopLocked(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !w.isRunning.Load() {
		return nil
	}

	var errs []error

	if w.manager != nil {
		currentState := w.manager.GetControlState()
		if !currentState.IsTerminal() {
			w.manager.RequestStop("Stop requested")
			if err := w.waitForManagerTerminalAndCleanup(ctx); err != nil {
				w.manager.RequestKill("Stop timeout")
				if killErr := w.waitForManagerTerminalAndCleanup(ctx); killErr != nil {
					errs = append(errs, fmt.Errorf("manager stop failed: %w; forced kill failed: %v", err, killErr))
				} else {
					errs = append(errs, fmt.Errorf("manager stop timed out; forced kill completed: %w", err))
				}
			}
		}
		w.manager.Close()
	}

	if err := w.stopAllWatches(ctx); err != nil {
		errs = append(errs, err)
	}

	if w.apiServer != nil {
		if err := w.apiServer.Shutdown(ctx); err != nil {
			if closeErr := w.apiServer.Close(); closeErr != nil {
				errs = append(errs, fmt.Errorf("api server shutdown error: %w; close error: %v", err, closeErr))
			} else {
				errs = append(errs, fmt.Errorf("api server shutdown error: %w", err))
			}
		}
		w.apiServer = nil
	}

	w.isRunning.Store(false)
	return errors.Join(errs...)
}

func (w *Watcher) runManager() error {
	if !w.isRunning.Load() {
		return ErrWatcherNotRunning
	}
	if w.manager == nil {
		return ErrNoManagedFunction
	}

	currentState := w.manager.GetControlState()

	if currentState.IsTerminal() {
		w.manager.RequestRun("run requested", true)
		return w.waitForStateType(&ControlIdle{}, 60*time.Second)
	}

	if _, ok := currentState.(*ControlIdle); ok {
		w.manager.RequestRun("watcher started", true)
		return nil
	}

	return fmt.Errorf("manager is already running (current state: %s)", currentState)
}

// StartWm starts a specific WatchMachine by varName.
func (w *Watcher) StartWm(varName string) error {
	machine, ok := w.machineRegistry.Load(varName)
	if !ok {
		return fmt.Errorf("watchMachine not found: %s", varName)
	}
	return machine.Start()
}

// StopWm stops a specific WatchMachine by varName.
func (w *Watcher) StopWm(varName string) error {
	machine, ok := w.machineRegistry.Load(varName)
	if !ok {
		return fmt.Errorf("watchMachine not found: %s", varName)
	}
	return machine.Stop()
}

// KillWm forcefully terminates a specific WatchMachine by varName.
func (w *Watcher) KillWm(varName string) error {
	machine, ok := w.machineRegistry.Load(varName)
	if !ok {
		return fmt.Errorf("watchMachine not found: %s", varName)
	}
	return machine.Kill()
}

// SendMessage sends a user message to the managed function.
func (w *Watcher) SendMessage(content string) error {
	if w.manager == nil {
		return ErrNoManagedFunction
	}
	if !w.isRunning.Load() {
		return ErrWatcherNotRunning
	}

	msg := &Message{
		Content:    content,
		ReceivedAt: time.Now(),
	}

	w.manager.SendUserEvent(msg)
	return nil
}

// State returns the current ControlState.
func (w *Watcher) State() (ControlState, error) {
	if w.manager == nil {
		return nil, ErrNoManagedFunction
	}
	return w.currentState(), nil
}

// Logger returns the Watcher's logger for inspection.
func (w *Watcher) Logger() (*manager.Logger, error) {
	if w.manager == nil {
		return nil, ErrNoManagedFunction
	}
	return w.manager.GetLogger(), nil
}

func (w *Watcher) currentState() ControlState {
	if w.manager == nil {
		return nil
	}
	return w.manager.GetControlState()
}

// GetManager returns the internal Manager for testing purposes.
func (w *Watcher) GetManager() *manager.Manager {
	return w.manager
}

// IsRunning returns whether the Watcher is currently running.
func (w *Watcher) IsRunning() bool {
	return w.isRunning.Load()
}

func (w *Watcher) waitForStateType(targetState ControlState, timeout time.Duration) error {
	targetType := fmt.Sprintf("%T", targetState)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			currentState := w.manager.GetControlState()
			if fmt.Sprintf("%T", currentState) == targetType {
				return nil
			}
			if _, ok := currentState.(*ControlCrashed); ok {
				if _, targetIsCrashed := targetState.(*ControlCrashed); !targetIsCrashed {
					return fmt.Errorf("manager crashed while waiting for state %s", targetState)
				}
			}
			if _, ok := currentState.(*ControlKilled); ok {
				if _, targetIsKilled := targetState.(*ControlKilled); !targetIsKilled {
					return fmt.Errorf("manager killed while waiting for state %s", targetState)
				}
			}
		case <-deadline:
			currentState := w.manager.GetControlState()
			return fmt.Errorf("timeout waiting for state %s (current: %s)", targetState, currentState)
		}
	}
}

func (w *Watcher) waitForManagerTerminalAndCleanup(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cs := w.manager.GetControlState()
			if cs.IsTerminal() && w.manager.IsCleanupCompleted() {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for manager cleanup (current: %s): %w", w.manager.GetControlState(), ctx.Err())
		}
	}
}

func (w *Watcher) waitForTerminalOrContext(ctx context.Context) (ControlState, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			state := w.currentState()
			if state == nil {
				return nil, ErrNoManagedFunction
			}
			if state.IsTerminal() {
				return state, nil
			}
		case <-ctx.Done():
			return w.currentState(), ctx.Err()
		}
	}
}

func (w *Watcher) stopAllWatches(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		var errs []error
		w.machineRegistry.Range(func(varName string, machine *wm.WatchMachine) bool {
			if err := machine.Stop(); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", varName, err))
			}
			return true
		})
		done <- errors.Join(errs...)
	}()

	select {
	case err := <-done:
		if err != nil {
			return err
		}
		fmt.Println("[Watcher] All watches stopped successfully")
		return nil
	case <-time.After(1 * time.Minute):
		return fmt.Errorf("some watches did not stop within 1min timeout")
	case <-ctx.Done():
		return fmt.Errorf("stopping watches cancelled: %w", ctx.Err())
	}
}

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
