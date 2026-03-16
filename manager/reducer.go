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
	Event           interface{} // SemanticEvent or EffectDrivenEvent
	NextState       StateSnapshot
	TriggeredSignal *shared.TriggeredSignal
}

// Reducer manages state transitions based on events.
// It implements priority-based event processing and absorbs the Commander role.
type Reducer struct {
	state   *ManagerState
	signals *SignalChannels
	logger  ReduceLogger
}

// ReduceLogger handles logging of state transitions.
type ReduceLogger interface {
	LogReduce(action ReduceAction)
	LogWatchError(varName string, phase WatchErrorPhase, err error)
	LogStateTransitionFault(from, to shared.ControlState, reason string, err error)
}

// NewReducer creates a new Reducer.
func NewReducer(state *ManagerState, signals *SignalChannels, logger ReduceLogger) *Reducer {
	return &Reducer{
		state:   state,
		signals: signals,
		logger:  logger,
	}
}

// Run starts the reducer loop with synchronous Target execution.
// Main loop: Wait for event → Reduce → Execute Effect on Target → process EffectDrivenEvent recursively.
func (r *Reducer) Run(ctx context.Context, target *Target) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("reducer panic: %v", rec)
			r.state.SetControlState(shared.ControlCrashed)

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
			r.processAvailableEvents(ctx, target)
		}
	}
}

// processAvailableEvents drains event channels respecting priority.
func (r *Reducer) processAvailableEvents(ctx context.Context, target *Target) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if !r.tryProcessNextEvent(target) {
				return
			}
		}
	}
}

// tryProcessNextEvent processes one event following priority order.
// Returns true if an event was processed.
func (r *Reducer) tryProcessNextEvent(target *Target) bool {
	cs := r.state.GetControlState()

	// Priority 0: ControlEvent (highest)
	select {
	case event := <-r.signals.ControlEventChan:
		r.reduceAndExecute(event, target)
		return true
	default:
	}

	// Priority 1: UserEvent
	if r.canProcessUserEvent(cs) {
		select {
		case event := <-r.signals.UserEventChan:
			r.reduceAndExecute(event, target)
			return true
		default:
		}
	}

	// Priority 2: VarSig (lowest)
	if r.canProcessVarEvent(cs) {
		select {
		case event := <-r.signals.VarSigChan:
			r.reduceAndExecute(event, target)
			return true
		default:
		}
	}

	return false
}

// reduceAndExecute performs the complete Reduce-Execute cycle for a SemanticEvent.
func (r *Reducer) reduceAndExecute(event interface{}, target *Target) {
	prevSnapshot := r.state.Snapshot()

	// 1. Reduce: update ControlState + determine Effect
	effect, triggeredSig := r.reduce(event)

	// 2. Log
	action := ReduceAction{
		PrevState:       prevSnapshot,
		Event:           event,
		NextState:       r.state.Snapshot(),
		TriggeredSignal: triggeredSig,
	}
	if r.logger != nil {
		r.logger.LogReduce(action)
	}

	// 3. No effect → done
	if effect == nil {
		return
	}

	// 4. Pass effect to Target
	drivenEvent := target.Execute(effect)
	if drivenEvent == nil {
		return
	}

	// 5. Process EffectDrivenEvent recursively
	r.reduceEffectDrivenEvent(drivenEvent, target)
}

// reduceEffectDrivenEvent processes an EffectDrivenEvent recursively.
func (r *Reducer) reduceEffectDrivenEvent(event EffectDrivenEvent, target *Target) {
	prevSnapshot := r.state.Snapshot()

	// 1. Reduce driven event: update ControlState + determine next Effect
	effect := r.reduceDriven(event)

	// 2. Log
	action := ReduceAction{
		PrevState: prevSnapshot,
		Event:     event,
		NextState: r.state.Snapshot(),
	}
	if r.logger != nil {
		r.logger.LogReduce(action)
	}

	// 3. No effect → recursion ends
	if effect == nil {
		return
	}

	// 4. Pass effect to Target
	drivenEvent := target.Execute(effect)
	if drivenEvent == nil {
		return
	}

	// 5. Continue recursion
	r.reduceEffectDrivenEvent(drivenEvent, target)
}

// reduce processes a SemanticEvent: updates ControlState and returns the Effect to execute.
// This absorbs the former EffectCommander's role.
func (r *Reducer) reduce(event interface{}) (Effect, *shared.TriggeredSignal) {
	switch e := event.(type) {
	case *UserMessageReceived:
		return r.reduceUserEvent(e)
	case *wm.DELETED_VarSig:
		return r.reduceVarEvent(e)
	case *ControlEvent:
		return r.reduceControlEvent(e)
	}
	return nil, nil
}

// reduceUserEvent handles UserMessageReceived.
func (r *Reducer) reduceUserEvent(event *UserMessageReceived) (Effect, *shared.TriggeredSignal) {
	r.state.UserState.SetMessage(event.UserMessage)
	r.state.SetControlState(shared.ControlRunDesired)

	triggeredSig := &shared.TriggeredSignal{IsUserSig: true}
	return &RunEffect{TriggeredSignal: triggeredSig}, triggeredSig
}

// reduceVarEvent handles VarSig with batching.
func (r *Reducer) reduceVarEvent(sig *wm.DELETED_VarSig) (Effect, *shared.TriggeredSignal) {
	updates := r.collectAndApplyVarSigs(sig)

	if len(updates) > 0 {
		r.state.VarState.BatchSet(updates)
		r.state.SetControlState(shared.ControlRunDesired)

		varNames := make([]string, 0, len(updates))
		for varName := range updates {
			varNames = append(varNames, varName)
		}
		triggeredSig := &shared.TriggeredSignal{VarSigNames: varNames}
		return &RunEffect{TriggeredSignal: triggeredSig}, triggeredSig
	}

	return nil, nil
}

// reduceControlEvent handles external ControlEvents.
func (r *Reducer) reduceControlEvent(event *ControlEvent) (Effect, *shared.TriggeredSignal) {
	currentCS := r.state.GetControlState()

	switch event.Kind {
	case StopRequested:
		// Already in terminal state? Ignore.
		if currentCS.IsTerminal() {
			return nil, nil
		}
		r.state.SetControlState(shared.ControlStopDesired)
		return &CleanupEffect{ForState: shared.ControlStopped}, &shared.TriggeredSignal{IsWatcherSig: true}

	case KillRequested:
		if currentCS.IsTerminal() {
			return nil, nil
		}
		r.state.SetControlState(shared.ControlKillDesired)
		return &CleanupEffect{ForState: shared.ControlKilled}, &shared.TriggeredSignal{IsWatcherSig: true}

	case RunRequested:
		// Only from terminal states
		if !currentCS.IsTerminal() && currentCS != shared.ControlIdle {
			return nil, nil
		}
		r.state.SetControlState(shared.ControlRunDesired)
		triggeredSig := &shared.TriggeredSignal{IsWatcherSig: true}
		return &RunEffect{
			NeedInit:        event.NeedInit,
			TriggeredSignal: triggeredSig,
		}, triggeredSig
	}

	return nil, nil
}

// reduceDriven processes an EffectDrivenEvent: updates ControlState and returns next Effect.
func (r *Reducer) reduceDriven(event EffectDrivenEvent) Effect {
	switch e := event.(type) {
	case *ExecutionCompleted:
		r.state.SetControlState(shared.ControlIdle)
		return nil // recursion ends: settled at Idle

	case *ErrorSuppressed:
		r.state.SetControlState(shared.ControlIdle)
		return nil // recursion ends: settled at Idle

	case *ExecutionFailed:
		r.state.SetControlState(shared.ControlRecoverDesired)
		return &RecoverEffect{}

	case *CleanupCompleted:
		r.state.SetControlState(e.ForState) // terminal state settlement
		return nil

	case *RecoveryReady:
		r.state.SetControlState(shared.ControlRunDesired)
		return &RunEffect{TriggeredSignal: &shared.TriggeredSignal{IsWatcherSig: true}}

	case *RecoveryExhausted:
		r.state.SetControlState(shared.ControlCrashed)
		return nil

	case *DirectKilled:
		r.state.SetControlState(shared.ControlKilled)
		return nil

	case *DirectCrashed:
		r.state.SetControlState(shared.ControlCrashed)
		return nil
	}

	return nil
}

// canProcessUserEvent checks if current state can process UserEvent.
func (r *Reducer) canProcessUserEvent(cs shared.ControlState) bool {
	return cs == shared.ControlIdle
}

// canProcessVarEvent checks if current state can process VarSig.
func (r *Reducer) canProcessVarEvent(cs shared.ControlState) bool {
	return cs == shared.ControlIdle
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
		isIndependent := varSigs[0].DELETED_ISStateIndependent

		if isIndependent {
			// State-independent (Flow): only apply the last signal
			lastSig := varSigs[len(varSigs)-1]

			currentHV, exists := r.state.VarState.Get(varName)
			if !exists {
				currentHV = shared.RawWatchValue{}
			}

			nextHV := lastSig.DELETED_VarUpdateFunc(currentHV)

			if nextHV.Error != nil {
				if r.logger != nil {
					r.logger.LogWatchError(varName, ErrorPhaseExecuteComputeFunc, nextHV.Error)
				}
				updates[varName] = shared.RawWatchValue{Value: nil, Error: nextHV.Error}
				continue
			}

			updates[varName] = nextHV

		} else {
			// State-dependent (Tick): apply all signals sequentially
			currentHV, exists := r.state.VarState.Get(varName)
			if !exists {
				currentHV = shared.RawWatchValue{}
			}

			for _, sig := range varSigs {
				nextHV := sig.DELETED_VarUpdateFunc(currentHV)
				if nextHV.Error != nil {
					if r.logger != nil {
						r.logger.LogWatchError(varName, ErrorPhaseExecuteComputeFunc, nextHV.Error)
					}
					currentHV = shared.RawWatchValue{Value: nil, Error: nextHV.Error}
					continue
				}
				currentHV = nextHV
			}

			updates[varName] = currentHV
		}
	}

	return updates
}
