package manager

import (
	"context"
	"fmt"
	"runtime"

	"github.com/HershyOrg/watch/shared"
)

// ReduceAction represents a state transition that occurred.
type ReduceAction struct {
	PrevState       StateSnapshot
	Event           interface{} // SemanticEvent or EffectDrivenEvent
	Effect          Effect      // Effect determined by Reduce (nil if no effect)
	NextState       StateSnapshot
	TriggeredSignal *shared.TriggeredSignal
}

// Reducer manages state transitions based on events.
// It implements priority-based event processing and absorbs the Commander role.
type Reducer struct {
	state   *ManagerState
	signals *SignalChannels
	logger  ReduceLogger
	manager *Manager // MachineRegistry 접근용
}

// ReduceLogger handles logging of state transitions.
type ReduceLogger interface {
	LogReduce(action ReduceAction)
	LogStateTransitionFault(from, to shared.ControlState, reason string, err error)
}

// NewReducer creates a new Reducer.
func NewReducer(state *ManagerState, signals *SignalChannels, logger ReduceLogger, manager *Manager) *Reducer {
	return &Reducer{
		state:   state,
		signals: signals,
		logger:  logger,
		manager: manager,
	}
}

// Run starts the reducer loop with synchronous Runner execution.
// Main loop: Wait for event → Reduce → Execute Effect on Runner → process EffectDrivenEvent recursively.
func (r *Reducer) Run(ctx context.Context, runner *MgrfuncRnner) (err error) {
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
			r.processAvailableEvents(ctx, runner)
		}
	}
}

// processAvailableEvents drains event channels respecting priority.
func (r *Reducer) processAvailableEvents(ctx context.Context, runner *MgrfuncRnner) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if !r.tryProcessNextEvent(runner) {
				return
			}
		}
	}
}

// tryProcessNextEvent processes one event following priority order.
// Returns true if an event was processed.
func (r *Reducer) tryProcessNextEvent(runner *MgrfuncRnner) bool {
	cs := r.state.GetControlState()

	// Priority 0: ControlEvent (highest)
	select {
	case event := <-r.signals.ControlEventChan:
		r.reduceAndExecute(event, runner)
		return true
	default:
	}

	// Priority 1: UserEvent
	if r.canProcessUserEvent(cs) {
		select {
		case event := <-r.signals.UserEventChan:
			r.reduceAndExecute(event, runner)
			return true
		default:
		}
	}

	// Priority 2: Var (Marker 기반)
	if r.canProcessVarEvent(cs) {
		// Drain: NewSigAppended 잔여 알림 소비
		r.drainNewSigAppended()

		// 각 WM에 ReadLatestFor 호출
		updates := r.readLatestFromAllWMs()
		if len(updates) > 0 {
			r.state.VarState.BatchSet(updates)
			r.state.SetControlState(shared.ControlRunDesired)

			varNames := make([]string, 0, len(updates))
			for v := range updates {
				varNames = append(varNames, v)
			}

			triggeredSig := &shared.TriggeredSignal{VarSigNames: varNames}
			r.reduceAndExecuteVar(triggeredSig, runner)
			return true
		}
		// 모든 WM이 (_, true) 반환 → 이미 최신 → 실행 안 함
	}

	return false
}

// reduceAndExecute performs the complete Reduce-Execute cycle for a SemanticEvent.
func (r *Reducer) reduceAndExecute(event interface{}, runner *MgrfuncRnner) {
	prevSnapshot := r.state.Snapshot()

	// 1. Reduce: update ControlState + determine Effect
	effect, triggeredSig := r.reduce(event)

	// 2. Log
	action := ReduceAction{
		PrevState:       prevSnapshot,
		Event:           event,
		Effect:          effect,
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

	// 4. Pass effect to MgrFuncRunner
	drivenEvent := runner.Execute(effect)
	if drivenEvent == nil {
		return
	}

	// 5. Process EffectDrivenEvent recursively
	r.reduceEffectDrivenEvent(drivenEvent, runner)
}

// reduceAndExecuteVar handles Var trigger: log and execute RunEffect.
func (r *Reducer) reduceAndExecuteVar(triggeredSig *shared.TriggeredSignal, runner *MgrfuncRnner) {
	prevSnapshot := r.state.Snapshot()
	effect := &RunEffect{TriggeredSignal: triggeredSig}

	action := ReduceAction{
		PrevState:       prevSnapshot,
		Event:           triggeredSig,
		Effect:          effect,
		NextState:       r.state.Snapshot(),
		TriggeredSignal: triggeredSig,
	}
	if r.logger != nil {
		r.logger.LogReduce(action)
	}

	drivenEvent := runner.Execute(effect)
	if drivenEvent != nil {
		r.reduceEffectDrivenEvent(drivenEvent, runner)
	}
}

// reduceEffectDrivenEvent processes an EffectDrivenEvent recursively.
func (r *Reducer) reduceEffectDrivenEvent(event EffectDrivenEvent, runner *MgrfuncRnner) {
	prevSnapshot := r.state.Snapshot()

	// 1. Reduce driven event: update ControlState + determine next Effect
	effect := r.reduceDriven(event)

	// 2. Log
	action := ReduceAction{
		PrevState: prevSnapshot,
		Event:     event,
		Effect:    effect,
		NextState: r.state.Snapshot(),
	}
	if r.logger != nil {
		r.logger.LogReduce(action)
	}

	// 3. No effect → recursion ends
	if effect == nil {
		return
	}

	// 4. Pass effect to Runner
	drivenEvent := runner.Execute(effect)
	if drivenEvent == nil {
		return
	}

	// 5. Continue recursion
	r.reduceEffectDrivenEvent(drivenEvent, runner)
}

// reduce processes a SemanticEvent: updates ControlState and returns the Effect to execute.
func (r *Reducer) reduce(event interface{}) (Effect, *shared.TriggeredSignal) {
	switch e := event.(type) {
	case *UserMessageReceived:
		return r.reduceUserEvent(e)
	case *ControlEvent:
		return r.reduceControlEvent(e)
	}
	return nil, nil
}

// reduceUserEvent handles UserMessageReceived.
func (r *Reducer) reduceUserEvent(event *UserMessageReceived) (Effect, *shared.TriggeredSignal) {
	r.state.SetControlState(shared.ControlRunDesired)

	triggeredSig := &shared.TriggeredSignal{IsUserSig: true}
	return &RunEffect{TriggeredSignal: triggeredSig, Message: event.UserMessage}, triggeredSig
}

// reduceControlEvent handles external ControlEvents.
func (r *Reducer) reduceControlEvent(event *ControlEvent) (Effect, *shared.TriggeredSignal) {
	currentCS := r.state.GetControlState()

	switch event.Kind {
	case StopRequested:
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
		return nil

	case *ErrorSuppressed:
		r.state.SetControlState(shared.ControlIdle)
		return nil

	case *ExecutionFailed:
		r.state.SetControlState(shared.ControlRecoverDesired)
		return &RecoverEffect{}

	case *CleanupCompleted:
		r.state.SetControlState(e.ForState)
		return nil

	case *RecoveryReady:
		r.state.SetControlState(shared.ControlRunDesired)
		return &RunEffect{TriggeredSignal: &shared.TriggeredSignal{IsWatcherSig: true}, NeedInit: true}

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

// Reduce is the exported version of reduce for testing.
func (r *Reducer) Reduce(event interface{}) (Effect, *shared.TriggeredSignal) {
	return r.reduce(event)
}

// ReduceDriven is the exported version of reduceDriven for testing.
func (r *Reducer) ReduceDriven(event EffectDrivenEvent) Effect {
	return r.reduceDriven(event)
}

// canProcessUserEvent checks if current state can process UserEvent.
func (r *Reducer) canProcessUserEvent(cs shared.ControlState) bool {
	return cs == shared.ControlIdle
}

// canProcessVarEvent checks if current state can process VarSig.
func (r *Reducer) canProcessVarEvent(cs shared.ControlState) bool {
	return cs == shared.ControlIdle
}

// drainNewSigAppended consumes all remaining NewSigAppended notifications.
func (r *Reducer) drainNewSigAppended() {
	for {
		select {
		case <-r.signals.NewSigAppended:
		default:
			return
		}
	}
}

// readLatestFromAllWMs reads the latest value from all WMs via MachineRegistry.
// Uses Marker-based index comparison: if subscriber already read latest, skip.
func (r *Reducer) readLatestFromAllWMs() map[string]shared.RawWatchValue {
	if r.manager == nil {
		return nil
	}
	registry := r.manager.GetMachineRegistry()
	if registry == nil {
		return nil
	}

	machines := registry.GetAllWatchMachines()
	managerName := r.manager.GetName()
	updates := make(map[string]shared.RawWatchValue)

	for _, machine := range machines {
		val, alreadyRead := machine.ReadLatestFor(managerName)
		if alreadyRead {
			continue
		}
		updates[machine.VarName] = val
	}

	if len(updates) == 0 {
		return nil
	}
	return updates
}
