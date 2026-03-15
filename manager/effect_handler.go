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
// It receives a message and HershContext, and returns an error for control flow.
type ManagedFunc func(message *shared.Message, ctx shared.ManageContext) error

// Cleaner provides cleanup functionality for managed functions.
type Cleaner interface {
	ClearRun(ctx shared.ManageContext) error
}

// EffectLogger logs effect execution results.
// It extends ContextLogger to include effect-specific logging.
type EffectLogger interface {
	ContextLogger // Embed ContextLogger interface
	LogEffectResult(result *EffectResult)
	GetRecentResults(count int) []*EffectResult
}

// EffectHandler executes effects and manages the lifecycle of managed functions.
// This is now a synchronous component - effects are executed via direct function calls.
type EffectHandler struct {
	mu               sync.RWMutex
	managedFunc      ManagedFunc
	cleaner          Cleaner
	state            *ManagerState
	signals          *SignalChannels
	logger           EffectLogger
	config           shared.WatcherConfig
	rootCtx          context.Context
	rootCtxCancel    context.CancelFunc
	manageCtx        *ManageContext // Persistent ManageContext across executions
	cleanupCompleted bool           // Tracks if cleanup has been completed
	manager          *Manager       // Reference to Manager for reinitialization
}

// NewEffectHandler creates a new EffectHandler.
func NewEffectHandler(
	managedFunc ManagedFunc,
	cleaner Cleaner,
	state *ManagerState,
	signals *SignalChannels,
	logger EffectLogger,
	config shared.WatcherConfig,
) *EffectHandler {
	bgCtx, cancel := context.WithCancel(context.Background())

	// Create persistent ManageContext
	// EffectLogger extends ContextLogger, so we can use it directly
	manageCtx := NewManageContext(bgCtx, logger)

	return &EffectHandler{
		managedFunc:      managedFunc,
		cleaner:          cleaner,
		state:            state,
		signals:          signals,
		logger:           logger,
		config:           config,
		rootCtx:          bgCtx,
		rootCtxCancel:    cancel,
		manageCtx:        manageCtx,
		cleanupCompleted: false,
		manager:          nil, // Set by Manager after creation
	}
}

// SetManager sets the Manager reference for reinitialization.
// This is called by NewManager after EffectHandler creation.
func (eh *EffectHandler) SetManager(mgr *Manager) {
	eh.mu.Lock()
	defer eh.mu.Unlock()
	eh.manager = mgr
}

// SetCleaner sets the cleaner function.
// This is used by CleanupBuilder to add cleanup logic after Manager creation.
func (eh *EffectHandler) SetCleaner(cleaner Cleaner) {
	eh.mu.Lock()
	defer eh.mu.Unlock()
	eh.cleaner = cleaner
}

// GetRootContext returns the rootCtx for Watch functions to use.
func (eh *EffectHandler) GetRootContext() context.Context {
	return eh.rootCtx
}

// GetManageContext returns the persistent HershContext.
func (eh *EffectHandler) GetManageContext() shared.ManageContext {
	return eh.manageCtx
}

// Reinitialize resets EffectHandler state for Manager restart.
// This is called when Manager transitions from terminal states to Running.
// - Cancels old rootCtx (stops all Watch goroutines)
// - Creates new rootCtx
// - Resets cleanup flag
// - Updates ManageContext with new rootCtx
// - Calls Manager.Reinitialize() to clear Manager state
func (eh *EffectHandler) Reinitialize(mgr *Manager) {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	// 1. Cancel old rootCtx (stops all Watch goroutines)
	if eh.rootCtxCancel != nil {
		eh.rootCtxCancel()
	}

	// 2. Create new rootCtx
	eh.rootCtx, eh.rootCtxCancel = context.WithCancel(context.Background())

	// 3. Reset cleanup flag
	eh.cleanupCompleted = false

	// 4. Update ManageContext with new rootCtx
	eh.manageCtx.UpdateContext(eh.rootCtx)

	// 5. Call Manager.Reinitialize() to clear Manager state
	// (VarState, WatchRegistry, MemoCache, VarSig channel)
	mgr.Reinitialize()
}

// ExecuteEffect executes an effect and returns the resulting WatcherSig (if any).
// This is called synchronously by the Reducer.
// Returns nil if no further state transition is needed.
func (eh *EffectHandler) ExecuteEffect(effect EffectDefinition) *ManagerInnerSig {
	return eh.executeEffect(effect)
}

// executeEffect executes the effect and returns the resulting WatcherSig.
// Returns nil if no state transition is needed.
func (eh *EffectHandler) executeEffect(effect EffectDefinition) *ManagerInnerSig {
	var result *EffectResult
	var sig *ManagerInnerSig

	switch e := effect.(type) {
	case *RunScriptEffect:
		result, sig = eh.runScript(e)
	case *ClearRunScriptEffect:
		result, sig = eh.clearRunScript(e.HookState)
	case *JustKillEffect:
		result, sig = eh.justKill()
	case *JustCrashEffect:
		result, sig = eh.justCrash()
	case *RecoverEffect:
		result, sig = eh.recover()
	default:
		result = &EffectResult{
			Effect:    effect,
			Success:   false,
			Error:     fmt.Errorf("unknown effect type: %T", effect),
			Timestamp: time.Now(),
		}
		sig = nil
	}

	if eh.logger != nil {
		eh.logger.LogEffectResult(result)
	}

	return sig
}

// runScript executes the managed function.
// Returns (result, sig) where sig is the state transition signal.
func (eh *EffectHandler) runScript(effect *RunScriptEffect) (*EffectResult, *ManagerInnerSig) {
	result := &EffectResult{
		Effect:    effect,
		Timestamp: time.Now(),
	}

	// Check if initialization is needed (restart scenario)
	if effect.NeedInit {
		eh.mu.RLock()
		mgr := eh.manager
		eh.mu.RUnlock()

		if mgr != nil {
			// Perform reinitialization
			eh.Reinitialize(mgr)
		}
	}

	// Create execution context with timeout from rootCtx
	// This ensures timeout propagates through all child contexts
	execCtx, cancel := context.WithTimeout(eh.rootCtx, eh.config.DefaultTimeout)
	defer cancel()

	// Consume message
	msg := eh.state.UserState.ConsumeMessage()

	// Update persistent HershContext with new context, message, and triggered signal
	// All Watch calls will use this context and respect the timeout
	eh.manageCtx.UpdateContext(execCtx)
	eh.manageCtx.SetMessage(msg)
	eh.manageCtx.SetTriggeredSignal(effect.TriggeredSignal)

	// Get managedFunc with read lock
	eh.mu.RLock()
	fn := eh.managedFunc
	eh.mu.RUnlock()

	// Execute in goroutine with panic recovery
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic: %v", r)
			}
		}()
		done <- fn(msg, eh.manageCtx)
	}()

	// Wait for completion or timeout
	// Priority: timeout signal takes precedence over goroutine completion
	var sig *ManagerInnerSig
	select {
	case <-execCtx.Done():
		// Timeout occurred - this is checked first for immediate response
		result.Success = false
		result.Error = execCtx.Err()
		sig = &ManagerInnerSig{
			ReceivedTime: time.Now(),
			TargetState:  shared.StateReady,
			Reason:       "execution timeout",
		}
		// Note: goroutine may still be running, but context cancellation
		// will be propagated to all child contexts (WatchCall, etc.)
	case err := <-done:
		// Execution completed before timeout
		if err != nil {
			result.Success = false
			result.Error = err
			sig = eh.handleScriptError(err)
		} else {
			result.Success = true
			sig = &ManagerInnerSig{
				ReceivedTime: time.Now(),
				TargetState:  shared.StateReady,
				Reason:       "execution completed successfully",
			}
		}
	}

	return result, sig
}

// handleScriptError processes errors from managed function execution.
// Returns the appropriate WatcherSig based on error type and recovery policy.
// Part 2: Determines state transition AFTER execution (delay was already applied in runScript)
func (eh *EffectHandler) handleScriptError(err error) *ManagerInnerSig {
	// Check if this is a panic error message
	if err != nil {
		errMsg := err.Error()
		// Check for WatchInitPanic pattern - these should crash immediately
		if strings.Contains(errMsg, "panic:") && strings.Contains(errMsg, "WatchInitPanic") {
			// This is a critical Watch initialization panic - crash immediately
			return &ManagerInnerSig{
				ReceivedTime: time.Now(),
				TargetState:  shared.StateCrashed,
				Reason:       fmt.Sprintf("Critical Watch initialization failure: %v", err),
			}
		}
	}

	switch err.(type) {
	case *shared.KillError:
		return &ManagerInnerSig{
			ReceivedTime: time.Now(),
			TargetState:  shared.StateKilled,
			Reason:       err.Error(),
		}
	case *shared.StopError:
		return &ManagerInnerSig{
			ReceivedTime: time.Now(),
			TargetState:  shared.StateStopped,
			Reason:       err.Error(),
		}
	default:
		// Count consecutive failures from recent logs
		consecutiveFails := eh.countConsecutiveFailures()

		if consecutiveFails < eh.config.RecoveryPolicy.MinConsecutiveFailures {
			// Apply lightweight retry delay
			delay := eh.calculateLightweightRetryDelay(consecutiveFails)
			if delay > 0 {
				time.Sleep(delay)
			}

			return &ManagerInnerSig{
				ReceivedTime: time.Now(),
				TargetState:  shared.StateReady,
				Reason: fmt.Sprintf("error suppressed (%d/%d) after %v delay: %v",
					consecutiveFails, eh.config.RecoveryPolicy.MinConsecutiveFailures, delay, err),
			}
		}

		// Too many consecutive failures - enter recovery mode
		return &ManagerInnerSig{
			ReceivedTime: time.Now(),
			TargetState:  shared.StateWaitRecover,
			Reason: fmt.Sprintf("consecutive failures (%d) >= threshold (%d): %v",
				consecutiveFails, eh.config.RecoveryPolicy.MinConsecutiveFailures, err),
		}
	}
}

// clearRunScript executes cleanup.
// Returns (result, sig).
func (eh *EffectHandler) clearRunScript(hookState shared.ManagerInnerState) (*EffectResult, *ManagerInnerSig) {
	result := &EffectResult{
		Effect:    &ClearRunScriptEffect{HookState: hookState},
		Timestamp: time.Now(),
	}

	// Cancel rootCtx before cleanup - this will stop all Watch goroutines
	eh.rootCtxCancel()

	// Execute cleanup using persistent HershContext
	if eh.cleaner != nil {
		// Update context with 5-minute timeout for cleanup
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		eh.manageCtx.UpdateContext(cleanCtx)

		err := eh.cleaner.ClearRun(eh.manageCtx)
		if err != nil {
			result.Success = false
			result.Error = err
		} else {
			result.Success = true
		}
	} else {
		result.Success = true
	}

	// Mark cleanup as completed
	eh.mu.Lock()
	// DO NOT create new rootCtx - Manager lifecycle is ending
	// Only create new rootCtx if Manager will be reused (which it won't be)
	eh.cleanupCompleted = true
	eh.mu.Unlock()

	// Return signal to transition to hook state
	sig := &ManagerInnerSig{
		ReceivedTime: time.Now(),
		TargetState:  hookState,
		Reason:       fmt.Sprintf("cleanup completed for %s", hookState),
	}

	return result, sig
}

// IsCleanupCompleted returns whether cleanup has been completed.
// This allows Watcher.Stop() to poll for cleanup completion.
// Returns true if no cleaner is set (cleanup not needed) or if cleanup has been completed.
func (eh *EffectHandler) IsCleanupCompleted() bool {
	eh.mu.RLock()
	defer eh.mu.RUnlock()
	// If no cleaner is set, consider cleanup as "completed" (not needed)
	if eh.cleaner == nil {
		return true
	}
	return eh.cleanupCompleted
}

// justKill returns Kill signal without cleanup.
// Returns (result, sig).
func (eh *EffectHandler) justKill() (*EffectResult, *ManagerInnerSig) {
	sig := &ManagerInnerSig{
		ReceivedTime: time.Now(),
		TargetState:  shared.StateKilled,
		Reason:       "kill requested",
	}
	result := &EffectResult{
		Effect:    &JustKillEffect{},
		Success:   true,
		Timestamp: time.Now(),
	}
	return result, sig
}

// justCrash returns Crash signal without cleanup.
// Returns (result, sig).
func (eh *EffectHandler) justCrash() (*EffectResult, *ManagerInnerSig) {
	sig := &ManagerInnerSig{
		ReceivedTime: time.Now(),
		TargetState:  shared.StateCrashed,
		Reason:       "crash requested",
	}
	result := &EffectResult{
		Effect:    &JustCrashEffect{},
		Success:   true,
		Timestamp: time.Now(),
	}
	return result, sig
}

// recover implements the recovery logic (Erlang Supervisor pattern).
// Returns (result, sig).
func (eh *EffectHandler) recover() (*EffectResult, *ManagerInnerSig) {
	result := &EffectResult{
		Effect:    &RecoverEffect{},
		Timestamp: time.Now(),
	}

	// Count consecutive failures from logs
	consecutiveFails := eh.countConsecutiveFailures()

	if consecutiveFails >= eh.config.RecoveryPolicy.MaxConsecutiveFailures {
		// Too many failures - crash
		result.Success = false
		result.Error = fmt.Errorf("max consecutive failures reached: %d", consecutiveFails)
		sig := &ManagerInnerSig{
			ReceivedTime: time.Now(),
			TargetState:  shared.StateCrashed,
			Reason:       "max consecutive failures exceeded",
		}
		return result, sig
	}

	// Calculate recovery backoff delay
	delay := eh.calculateRecoveryBackoff(consecutiveFails)
	time.Sleep(delay)

	// Attempt recovery - return Running signal (no more InitRun)
	result.Success = true
	sig := &ManagerInnerSig{
		ReceivedTime: time.Now(),
		TargetState:  shared.StateRunning,
		Reason:       fmt.Sprintf("recovery attempt after %d failures (backoff: %v)", consecutiveFails, delay),
	}

	return result, sig
}

// countConsecutiveFailures counts recent consecutive failures from logs.
// Used by handleScriptError to include the current failure (+1).
func (eh *EffectHandler) countConsecutiveFailures() int {
	// Get recent results from log
	recentResults := eh.logger.GetRecentResults(eh.config.RecoveryPolicy.MaxConsecutiveFailures + 1)

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
func (eh *EffectHandler) calculateRecoveryBackoff(failures int) time.Duration {
	delay := eh.config.RecoveryPolicy.BaseRetryDelay

	// Recovery 진입 이후의 실패 횟수만 계산
	recoveryAttempts := failures - eh.config.RecoveryPolicy.MinConsecutiveFailures
	if recoveryAttempts < 0 {
		recoveryAttempts = 0
	}

	for i := 0; i < recoveryAttempts; i++ {
		delay *= 2
		if delay > eh.config.RecoveryPolicy.MaxRetryDelay {
			return eh.config.RecoveryPolicy.MaxRetryDelay
		}
	}
	return delay
}

// calculateLightweightRetryDelay calculates delay for failures below MinConsecutiveFailures.
// Returns 0 if no delay configured.
func (eh *EffectHandler) calculateLightweightRetryDelay(failures int) time.Duration {
	delays := eh.config.RecoveryPolicy.LightweightRetryDelays
	if len(delays) == 0 {
		return 0 // No lightweight retry configured (legacy behavior)
	}

	// failures는 1부터 시작 (첫 실패 = 1)
	index := failures - 1
	if index < 0 {
		return 0
	}
	if index >= len(delays) {
		// 마지막 delay 재사용
		index = len(delays) - 1
	}

	return delays[index]
}

// calculateBackoff calculates exponential backoff delay (deprecated - use calculateRecoveryBackoff).
func (eh *EffectHandler) calculateBackoff(failures int) time.Duration {
	delay := eh.config.RecoveryPolicy.BaseRetryDelay
	for range failures {
		delay *= 2
		if delay > eh.config.RecoveryPolicy.MaxRetryDelay {
			return eh.config.RecoveryPolicy.MaxRetryDelay
		}
	}
	return delay
}
