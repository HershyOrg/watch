package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/HershyOrg/watch/shared"
)

// ManagedFunc is the type of function that can be managed by the Watcher.
// It receives a message and ManageContext, and returns an error for control flow.
type ManagedFunc func(message *shared.Message, ctx shared.ManageContext) error

// Cleaner provides cleanup functionality for managed functions.
type Cleaner interface {
	ClearRun(ctx shared.ManageContext) error
}

// EffectLogger logs effect execution results.
// It extends ContextLogger to include effect-specific logging.
type EffectLogger interface {
	ContextLogger
	LogEffectResult(result *EffectResult)
	GetRecentResults(count int) []*EffectResult
}

// MgrfuncRnner is the execution unit controlled by the Manager's Reducer.
// It owns its own MgrFuncRunnerState and processes Effects autonomously,
// returning EffectDrivenEvents that are processed recursively by the Reducer.
type MgrfuncRnner struct {
	mu               sync.RWMutex
	state            shared.RunnerState
	managedFunc      ManagedFunc
	cleaner          Cleaner
	config           shared.WatcherConfig
	manageCtx        *ManageContext
	rootCtx          context.Context
	rootCtxCancel    context.CancelFunc
	cleanupCompleted bool
	manager          *Manager
	logger           EffectLogger
}

// NewRunner creates a new MgrFuncRunner.
func NewRunner(
	managedFunc ManagedFunc,
	cleaner Cleaner,
	logger EffectLogger,
	config shared.WatcherConfig,
) *MgrfuncRnner {
	bgCtx, cancel := context.WithCancel(context.Background())
	manageCtx := NewManageContext(bgCtx, logger)

	return &MgrfuncRnner{
		state:            shared.RunnerIdle,
		managedFunc:      managedFunc,
		cleaner:          cleaner,
		logger:           logger,
		config:           config,
		rootCtx:          bgCtx,
		rootCtxCancel:    cancel,
		manageCtx:        manageCtx,
		cleanupCompleted: false,
		manager:          nil,
	}
}

// SetManager sets the Manager reference for reinitialization.
func (t *MgrfuncRnner) SetManager(mgr *Manager) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.manager = mgr
}

// SetCleaner sets the cleaner function.
func (t *MgrfuncRnner) SetCleaner(cleaner Cleaner) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cleaner = cleaner
}

// GetRootContext returns the rootCtx for Watch functions to use.
func (t *MgrfuncRnner) GetRootContext() context.Context {
	return t.rootCtx
}

// GetManageContext returns the persistent ManageContext.
func (t *MgrfuncRnner) GetManageContext() shared.ManageContext {
	return t.manageCtx
}

// GetRunnerState returns the current MgrFuncRunnerState.
func (t *MgrfuncRnner) GetRunnerState() shared.RunnerState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// IsCleanupCompleted returns whether cleanup has been completed.
func (t *MgrfuncRnner) IsCleanupCompleted() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.cleaner == nil {
		return true
	}
	return t.cleanupCompleted
}

// Reinitialize resets MgrFuncRunner state for Manager restart.
func (t *MgrfuncRnner) Reinitialize(mgr *Manager) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1. Cancel old rootCtx (stops all Watch goroutines)
	if t.rootCtxCancel != nil {
		t.rootCtxCancel()
	}

	// 2. Create new rootCtx
	t.rootCtx, t.rootCtxCancel = context.WithCancel(context.Background())

	// 3. Reset cleanup flag
	t.cleanupCompleted = false

	// 4. Update ManageContext with new rootCtx
	t.manageCtx.UpdateContext(t.rootCtx)

	// 5. Call Manager.Reinitialize() to clear Manager state
	mgr.Reinitialize()
}

// Execute processes an Effect based on the MgrFuncRunner's own MgrFuncRunnerState.
// Returns an EffectDrivenEvent describing what happened.
func (t *MgrfuncRnner) Execute(effect Effect) EffectDrivenEvent {
	var result *EffectResult
	var drivenEvent EffectDrivenEvent

	switch e := effect.(type) {
	case *RunEffect:
		result, drivenEvent = t.executeRun(e)
	case *CleanupEffect:
		result, drivenEvent = t.executeCleanup(e)
	case *RecoverEffect:
		result, drivenEvent = t.executeRecover()
	case *DirectKillEffect:
		result, drivenEvent = t.executeDirectKill()
	case *DirectCrashEffect:
		result, drivenEvent = t.executeDirectCrash()
	default:
		result = &EffectResult{
			Effect:    effect,
			Success:   false,
			Error:     fmt.Errorf("unknown effect type: %T", effect),
			Timestamp: time.Now(),
		}
	}

	if t.logger != nil && result != nil {
		t.logger.LogEffectResult(result)
	}

	return drivenEvent
}

// executeRun executes the managed function.
func (t *MgrfuncRnner) executeRun(effect *RunEffect) (*EffectResult, EffectDrivenEvent) {
	result := &EffectResult{
		Effect:    effect,
		Timestamp: time.Now(),
	}

	// Check if initialization is needed (restart scenario)
	if effect.NeedInit {
		t.mu.RLock()
		mgr := t.manager
		t.mu.RUnlock()

		if mgr != nil {
			t.Reinitialize(mgr)
		}
	}

	// MgrFuncRunnerState → Running
	t.mu.Lock()
	t.state = shared.RunnerRunning
	t.mu.Unlock()

	// Create execution context with timeout from rootCtx
	execCtx, cancel := context.WithTimeout(t.rootCtx, t.config.DefaultTimeout)
	defer cancel()

	// Get message from effect
	msg := effect.Message

	// Update persistent ManageContext
	t.manageCtx.UpdateContext(execCtx)
	t.manageCtx.SetMessage(msg)
	t.manageCtx.SetTriggeredSignal(effect.TriggeredSignal)

	// Get managedFunc with read lock
	t.mu.RLock()
	fn := t.managedFunc
	t.mu.RUnlock()

	// Execute in goroutine with panic recovery
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic: %v", r)
			}
		}()
		done <- fn(msg, t.manageCtx)
	}()

	// Wait for completion or timeout
	var drivenEvent EffectDrivenEvent
	select {
	case <-execCtx.Done():
		// Timeout occurred
		result.Success = false
		result.Error = execCtx.Err()

		// MgrFuncRunnerState → Idle (matches original behavior: timeout → Ready without recovery)
		t.mu.Lock()
		t.state = shared.RunnerIdle
		t.mu.Unlock()

		drivenEvent = &ErrorSuppressed{}

	case err := <-done:
		if err != nil {
			result.Success = false
			result.Error = err
			drivenEvent = t.handleScriptError(err)
		} else {
			result.Success = true
			// MgrFuncRunnerState → Idle
			t.mu.Lock()
			t.state = shared.RunnerIdle
			t.mu.Unlock()
			drivenEvent = &ExecutionCompleted{}
		}
	}

	return result, drivenEvent
}

// handleScriptError processes errors from managed function execution.
func (t *MgrfuncRnner) handleScriptError(err error) EffectDrivenEvent {
	// Check for WatchInitPanic pattern - crash immediately
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "panic:") && strings.Contains(errMsg, "WatchInitPanic") {
			t.mu.Lock()
			t.state = shared.RunnerCrashed
			t.mu.Unlock()
			return &DirectCrashed{}
		}
	}

	switch err.(type) {
	case *shared.KillError:
		// KillError requires cleanup before transitioning to Killed
		_, cleanupEvent := t.executeCleanup(&CleanupEffect{ForState: shared.ControlKilled})
		return cleanupEvent

	case *shared.StopError:
		// StopError requires cleanup before transitioning to Stopped
		_, cleanupEvent := t.executeCleanup(&CleanupEffect{ForState: shared.ControlStopped})
		return cleanupEvent

	default:
		// Count consecutive failures
		consecutiveFails := t.countConsecutiveFailures()

		if consecutiveFails < t.config.RecoveryPolicy.MinConsecutiveFailures {
			// Lightweight retry: apply delay then suppress
			delay := t.calculateLightweightRetryDelay(consecutiveFails)
			if delay > 0 {
				t.mu.Lock()
				t.state = shared.RunnerSleeping
				t.mu.Unlock()

				time.Sleep(delay)
			}

			t.mu.Lock()
			t.state = shared.RunnerIdle
			t.mu.Unlock()

			return &ErrorSuppressed{}
		}

		// Too many consecutive failures - request recovery
		t.mu.Lock()
		t.state = shared.RunnerIdle
		t.mu.Unlock()

		return &ExecutionFailed{
			Err:      err,
			Failures: consecutiveFails,
		}
	}
}

// executeCleanup executes cleanup.
func (t *MgrfuncRnner) executeCleanup(effect *CleanupEffect) (*EffectResult, EffectDrivenEvent) {
	result := &EffectResult{
		Effect:    effect,
		Timestamp: time.Now(),
	}

	// Cancel rootCtx before cleanup - stops all Watch goroutines
	t.rootCtxCancel()

	// MgrFuncRunnerState → CleaningUp
	t.mu.Lock()
	t.state = shared.RunnerCleaningUp
	t.mu.Unlock()

	// Execute cleanup using persistent ManageContext
	if t.cleaner != nil {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		t.manageCtx.UpdateContext(cleanCtx)

		err := t.cleaner.ClearRun(t.manageCtx)
		if err != nil {
			result.Success = false
			result.Error = err
		} else {
			result.Success = true
		}
	} else {
		result.Success = true
	}

	// Update MgrFuncRunnerState based on ForState
	t.mu.Lock()
	switch effect.ForState {
	case shared.ControlStopped:
		t.state = shared.RunnerStopped
	case shared.ControlKilled:
		t.state = shared.RunnerKilled
	case shared.ControlCrashed:
		t.state = shared.RunnerCrashed
	default:
		t.state = shared.RunnerStopped
	}
	t.cleanupCompleted = true
	t.mu.Unlock()

	return result, &CleanupCompleted{ForState: effect.ForState}
}

// executeRecover implements the recovery logic (Erlang Supervisor pattern).
func (t *MgrfuncRnner) executeRecover() (*EffectResult, EffectDrivenEvent) {
	result := &EffectResult{
		Effect:    &RecoverEffect{},
		Timestamp: time.Now(),
	}

	consecutiveFails := t.countConsecutiveFailures()

	if consecutiveFails >= t.config.RecoveryPolicy.MaxConsecutiveFailures {
		// Too many failures - crash
		result.Success = false
		result.Error = fmt.Errorf("max consecutive failures reached: %d", consecutiveFails)

		t.mu.Lock()
		t.state = shared.RunnerCrashed
		t.mu.Unlock()

		return result, &RecoveryExhausted{}
	}

	// Calculate recovery backoff delay
	delay := t.calculateRecoveryBackoff(consecutiveFails)

	// MgrFuncRunnerState → Sleeping
	t.mu.Lock()
	t.state = shared.RunnerSleeping
	t.mu.Unlock()

	time.Sleep(delay)

	// MgrFuncRunnerState → Idle (ready for retry)
	t.mu.Lock()
	t.state = shared.RunnerIdle
	t.mu.Unlock()

	result.Success = true
	return result, &RecoveryReady{}
}

// executeDirectKill transitions to Killed without cleanup.
func (t *MgrfuncRnner) executeDirectKill() (*EffectResult, EffectDrivenEvent) {
	t.mu.Lock()
	t.state = shared.RunnerKilled
	t.mu.Unlock()

	result := &EffectResult{
		Effect:    &DirectKillEffect{},
		Success:   true,
		Timestamp: time.Now(),
	}
	return result, &DirectKilled{}
}

// executeDirectCrash transitions to Crashed without cleanup.
func (t *MgrfuncRnner) executeDirectCrash() (*EffectResult, EffectDrivenEvent) {
	t.mu.Lock()
	t.state = shared.RunnerCrashed
	t.mu.Unlock()

	result := &EffectResult{
		Effect:    &DirectCrashEffect{},
		Success:   true,
		Timestamp: time.Now(),
	}
	return result, &DirectCrashed{}
}

// countConsecutiveFailures counts recent consecutive failures from logs.
func (t *MgrfuncRnner) countConsecutiveFailures() int {
	recentResults := t.logger.GetRecentResults(t.config.RecoveryPolicy.MaxConsecutiveFailures + 1)

	consecutiveFails := 0
	for i := len(recentResults) - 1; i >= 0; i-- {
		if !recentResults[i].Success {
			consecutiveFails++
		} else {
			break
		}
	}

	// +1 for current failure (not yet logged)
	return consecutiveFails + 1
}

// calculateRecoveryBackoff calculates exponential backoff for recovery attempts.
func (t *MgrfuncRnner) calculateRecoveryBackoff(failures int) time.Duration {
	delay := t.config.RecoveryPolicy.BaseRetryDelay

	recoveryAttempts := failures - t.config.RecoveryPolicy.MinConsecutiveFailures
	if recoveryAttempts < 0 {
		recoveryAttempts = 0
	}

	for i := 0; i < recoveryAttempts; i++ {
		delay *= 2
		if delay > t.config.RecoveryPolicy.MaxRetryDelay {
			return t.config.RecoveryPolicy.MaxRetryDelay
		}
	}
	return delay
}

// calculateLightweightRetryDelay calculates delay for failures below MinConsecutiveFailures.
func (t *MgrfuncRnner) calculateLightweightRetryDelay(failures int) time.Duration {
	delays := t.config.RecoveryPolicy.LightweightRetryDelays
	if len(delays) == 0 {
		return 0
	}

	index := failures - 1
	if index < 0 {
		return 0
	}
	if index >= len(delays) {
		index = len(delays) - 1
	}

	return delays[index]
}
