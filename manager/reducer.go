package manager

import (
	"context"
	"fmt"
	"runtime"

	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// ReduceAction represents a state transition that occurred.
type ReduceAction struct {
	PrevState       StateSnapshot
	Signal          shared.Signal
	NextState       StateSnapshot
	TriggeredSignal *shared.TriggeredSignal // Information about which signals triggered this action
}

// Reducer manages state transitions based on signals.
// It implements priority-based signal processing.
type Reducer struct {
	state   *ManagerState
	signals *SignalChannels
	logger  ReduceLogger
}

// ReduceLogger handles logging of state transitions.
type ReduceLogger interface {
	LogReduce(action ReduceAction)
	LogWatchError(varName string, phase WatchErrorPhase, err error)
	LogStateTransitionFault(from, to shared.ManagerInnerState, reason string, err error)
}

// NewReducer creates a new Reducer.
func NewReducer(state *ManagerState, signals *SignalChannels, logger ReduceLogger) *Reducer {
	return &Reducer{
		state:   state,
		signals: signals,
		logger:  logger,
	}
}

// RunWithEffects starts the reducer loop with synchronous effect execution.
// This is the main loop following the specification:
// 1. Wait for signal
// 2. Reduce (state transition)
// 3. Call EffectCommander synchronously
// 4. Call EffectHandler synchronously
// 5. If effect returns WatcherSig, process it recursively
// Priority: WatcherSig > UserSig > VarSig
func (r *Reducer) RunWithEffects(ctx context.Context, commander *EffectCommander, handler *EffectHandler) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			// Panic recovery
			err = fmt.Errorf("reducer panic: %v", rec)

			// Transition to Crashed state
			r.state.SetManagerInnerState(shared.StateCrashed)

			// Log panic with stack trace
			fmt.Printf("[Reducer] PANIC RECOVERED: %v\n", rec)
			fmt.Printf("[Reducer] Stack trace:\n")

			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			fmt.Printf("%s\n", buf[:n])
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.signals.NewSigAppended:
			// Process all available signals respecting priority
			r.processAvailableSignalsWithEffects(ctx, commander, handler)
		}
	}
}

// processAvailableSignalsWithEffects drains signal channels respecting priority.
// For each signal: Reduce → CommandEffect → ExecuteEffect → handle result WatcherSig.
func (r *Reducer) processAvailableSignalsWithEffects(ctx context.Context, commander *EffectCommander, handler *EffectHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Try to process one signal, highest priority first
			if !r.tryProcessNextSignalWithEffects(commander, handler) {
				// No more signals to process
				return
			}
		}
	}
}

// tryProcessNextSignalWithEffects processes one signal following the specification:
// 1. Select signal by priority
// 2. Reduce (state transition)
// 3. CommandEffect (synchronous)
// 4. ExecuteEffect (synchronous)
// 5. If effect returns WatcherSig, process it recursively (atomic execution)
// Returns true if a signal was processed, false if no signals available.
func (r *Reducer) tryProcessNextSignalWithEffects(commander *EffectCommander, handler *EffectHandler) bool {
	currentState := r.state.GetManagerInnerState()

	// Priority 1: WatcherSig (highest)
	select {
	case sig := <-r.signals.ManagerInnerSigChan:
		r.reduceAndExecuteEffect(sig, commander, handler)
		return true
	default:
	}

	// Priority 2: UserSig
	if r.canProcessUserSig(currentState) {
		select {
		case sig := <-r.signals.UserSigChan:
			r.reduceAndExecuteEffect(sig, commander, handler)
			return true
		default:
		}
	}

	// Priority 3: VarSig (lowest)
	if r.canProcessVarSig(currentState) {
		select {
		case sig := <-r.signals.VarSigChan:
			r.reduceAndExecuteEffect(sig, commander, handler)
			return true
		default:
		}
	}

	return false
}

// reduceAndExecuteEffect performs the complete Reduce-Effect cycle:
// Reduce → CommandEffect → ExecuteEffect → handle result.
func (r *Reducer) reduceAndExecuteEffect(sig shared.Signal, commander *EffectCommander, handler *EffectHandler) {
	// 1. Reduce: perform state transition and get triggered signal info
	prevSnapshot := r.state.Snapshot()
	var triggeredSig *shared.TriggeredSignal

	switch s := sig.(type) {
	case *ManagerInnerSig:
		triggeredSig = r.reduceManagerInnerSig(s)
	case *UserSig:
		triggeredSig = r.reduceUserSig(s)
	case *wm.DELETED_VarSig:
		triggeredSig = r.reduceVarSig(s)
		// Note: InitRun completion check moved after effect execution for atomic processing
	default:
		return // Unknown signal type
	}

	// 2. Create action with triggered signal information
	action := ReduceAction{
		PrevState:       prevSnapshot,
		Signal:          sig,
		NextState:       r.state.Snapshot(),
		TriggeredSignal: triggeredSig,
	}

	// Log the reduce action
	if r.logger != nil {
		r.logger.LogReduce(action)
	}

	// 4. CommandEffect (synchronous)
	effectDef := commander.CommandEffect(action)
	if effectDef == nil {
		return // No effect to execute
	}

	// 5. ExecuteEffect (synchronous)
	resultSig := handler.ExecuteEffect(effectDef)
	if resultSig == nil {
		return // No further state transition needed
	}

	// 6. Process result WatcherSig recursively (atomic execution)
	r.reduceAndExecuteEffect(resultSig, commander, handler)
}

// canProcessUserSig checks if current state can process UserSig.
func (r *Reducer) canProcessUserSig(state shared.ManagerInnerState) bool {
	return state == shared.StateReady
}

// canProcessVarSig checks if current state can process VarSig.
func (r *Reducer) canProcessVarSig(state shared.ManagerInnerState) bool {
	return state == shared.StateReady
}

// reduceVarSig handles VarSig according to transition rules.
// Only called when canProcessVarSig returns true.
// Logging is handled by reduceAndExecuteEffect.
// Returns TriggeredSignal with the names of variables that were triggered.
func (r *Reducer) reduceVarSig(sig *wm.DELETED_VarSig) *shared.TriggeredSignal {
	currentState := r.state.GetManagerInnerState()

	switch currentState {
	case shared.StateReady:
		// Batch collect and apply all VarSigs
		updates := r.collectAndApplyVarSigs(sig)

		// Only transition to Running if there are actual updates
		if len(updates) > 0 {
			r.state.VarState.BatchSet(updates)
			r.state.SetManagerInnerState(shared.StateRunning)

			// Collect triggered variable names
			varNames := make([]string, 0, len(updates))
			for varName := range updates {
				varNames = append(varNames, varName)
			}
			return &shared.TriggeredSignal{VarSigNames: varNames}
		}
		// If no updates (all changed=false), stay in Ready state
		return nil

	default:
		// Should never reach here due to canProcessVarSig check
		panic(fmt.Sprintf("reduceVarSig called in invalid state: %s", currentState))
	}
}

// collectAndApplyVarSigs collects all VarSigs and applies them correctly.
// For IsStateIndependent=true (Flow): only apply the last signal's function
// For IsStateIndependent=false (Tick): apply all functions sequentially
func (r *Reducer) collectAndApplyVarSigs(first *wm.DELETED_VarSig) map[string]shared.RawWatchValue {
	sigs := []*wm.DELETED_VarSig{first}

	// Collect all available VarSigs from the channel
	for {
		select {
		case sig := <-r.signals.VarSigChan:
			sigs = append(sigs, sig)
		default:
			// break사용이 불가하므로 goto이용.
			goto APPLY
		}
	}

APPLY:
	// Group signals by variable name
	byVar := make(map[string][]*wm.DELETED_VarSig)
	for _, sig := range sigs {
		byVar[sig.TargetVarName] = append(byVar[sig.TargetVarName], sig)
	}

	updates := make(map[string]shared.RawWatchValue)

	for varName, varSigs := range byVar {
		// Check if this variable is state-independent (check first signal)
		isIndependent := varSigs[0].DELETED_ISStateIndependent

		if isIndependent {
			// State-independent (Flow): only apply the last signal
			lastSig := varSigs[len(varSigs)-1]

			// Get current RawHershValue from VarState
			currentHV, exists := r.state.VarState.Get(varName)
			if !exists {
				currentHV = shared.RawWatchValue{} // Empty RawHershValue if not exists
			}

			nextHV := lastSig.DELETED_VarUpdateFunc(currentHV)

			if nextHV.Error != nil {
				if r.logger != nil {
					r.logger.LogWatchError(varName, ErrorPhaseExecuteComputeFunc, nextHV.Error)
				}
				updates[varName] = shared.RawWatchValue{Value: nil, Error: nextHV.Error}
				continue
			}

			// Always update since user explicitly sent signal
			updates[varName] = nextHV

		} else {
			// State-dependent (Tick): apply all signals sequentially
			currentHV, exists := r.state.VarState.Get(varName)
			if !exists {
				currentHV = shared.RawWatchValue{} // Empty RawHershValue if not exists
			}

			for _, sig := range varSigs {
				nextHV := sig.DELETED_VarUpdateFunc(currentHV)
				if nextHV.Error != nil {
					// VarUpdateFunc execution error - log and store error in RawHershValue
					if r.logger != nil {
						r.logger.LogWatchError(varName, ErrorPhaseExecuteComputeFunc, nextHV.Error)
					}
					// Store the error
					currentHV = shared.RawWatchValue{Value: nil, Error: nextHV.Error}
					continue
				}
				currentHV = nextHV // Next function's input
			}

			// Always update since user explicitly sent signals
			updates[varName] = currentHV
		}
	}

	return updates
}

// reduceUserSig handles UserSig according to transition rules.
// Only called when canProcessUserSig returns true.
// Logging is handled by reduceAndExecuteEffect.
// Returns TriggeredSignal indicating UserSig was triggered.
func (r *Reducer) reduceUserSig(sig *UserSig) *shared.TriggeredSignal {
	currentState := r.state.GetManagerInnerState()

	switch currentState {
	case shared.StateReady:
		r.state.UserState.SetMessage(sig.UserMessage)
		r.state.SetManagerInnerState(shared.StateRunning)
		return &shared.TriggeredSignal{IsUserSig: true}

	default:
		// Should never reach here due to canProcessUserSig check
		panic(fmt.Sprintf("reduceUserSig called in invalid state: %s", currentState))
	}
}

// reduceManagerInnerSig handles WatcherSig according to transition rules.
// Logging is handled by reduceAndExecuteEffect.
// Returns TriggeredSignal indicating WatcherSig was triggered.
func (r *Reducer) reduceManagerInnerSig(sig *ManagerInnerSig) *shared.TriggeredSignal {
	currentState := r.state.GetManagerInnerState()
	targetState := sig.TargetState

	// Ignore same-state transitions
	if currentState == targetState {
		return nil
	}

	// Validate transition
	if err := r.validateTransition(currentState, targetState); err != nil {
		// Log error but don't crash reducer
		if r.logger != nil {
			r.logger.LogStateTransitionFault(currentState, targetState, sig.Reason, err)
		}
		return nil
	}

	// Special case: Crashed is terminal
	if currentState == shared.StateCrashed {
		if r.logger != nil {
			r.logger.LogStateTransitionFault(
				currentState,
				targetState,
				sig.Reason,
				fmt.Errorf("cannot transition from Crashed state"),
			)
		}
		return nil
	}

	// Perform transition
	r.state.SetManagerInnerState(targetState)
	return &shared.TriggeredSignal{IsWatcherSig: true}
}

// validateTransition checks if a state transition is valid.
func (r *Reducer) validateTransition(from, to shared.ManagerInnerState) error {
	// Some basic validation - full FSM rules would go here
	switch from {
	case shared.StateStopped:
		// From Stopped, allow Running (restart), Killed, Crashed, WaitRecover
		if to != shared.StateRunning && to != shared.StateKilled && to != shared.StateCrashed && to != shared.StateWaitRecover {
			return fmt.Errorf("invalid transition from Stopped to %s", to)
		}
	case shared.StateKilled:
		// From Killed, allow Running (restart), Crashed, WaitRecover
		if to != shared.StateRunning && to != shared.StateCrashed && to != shared.StateWaitRecover {
			return fmt.Errorf("invalid transition from Killed to %s", to)
		}
	case shared.StateCrashed:
		// From Crashed, allow Running (restart) - removed terminal constraint
		if to != shared.StateRunning {
			return fmt.Errorf("invalid transition from Crashed to %s", to)
		}
	}

	return nil
}
