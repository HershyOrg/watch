package watch

import (
	"context"
	"fmt"
	"time"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// getManagerFromContext extracts the Manager from ManageContext.
func getManagerFromContext(ctx shared.ManageContext) *manager.Manager {
	// ManageContext is now *manager.ManageContext
	if mc, ok := ctx.(*manager.ManageContext); ok {
		return mc.GetManager()
	}
	return nil
}

// ! Now, this DELETED_WatchCall won't be used any more
// ! It Will REPLACED by new Func "WatchCall"
// DELELTED_WatchCall monitors a value by periodically generating computation functions (generic version).
// Returns the current HershValue[T] with the initial value on first call.
//
// The init parameter provides the initial value before any updates occur.
//
// The getComputationFunc is called on each tick and returns:
// - A VarUpdateFunc[T] that computes the next state from the previous state
// - skipSignal: whether to skip sending a signal (default false = send signal)
// - An error if the computation function cannot be generated
//
// The returned VarUpdateFunc[T] receives:
// - prev: the previous value of type T (initial value on first call)
//
// The VarUpdateFunc[T] returns:
// - next: the new value of type T
// - error: any error that occurred during computation
func DELELTED_WatchCall[T any](
	init T,
	getComputationFunc func() (wm.DELETED_VarUpdateFunc[T], bool, error),
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	mgr := getManagerFromContext(runCtx)
	if mgr == nil {
		panic(shared.NewWatchInitPanic(
			varName,
			"WatchCall called with invalid ManageContext",
			nil,
		))
	}

	watchRegistry := mgr.GetWatchRegistry()
	_, exists := watchRegistry.Load(varName)

	if !exists {
		// First call - set initial value in VarState
		initialRaw := shared.RawWatchValue{
			Value:      any(init),
			Error:      nil,
			VarName:    varName,
			NotUpdated: true, // Mark as initial value
		}
		mgr.GetState().VarState.Set(varName, initialRaw)

		// Register and start watching - use Manager's EffectHandler rootCtx
		ctx, cancel := context.WithCancel(mgr.GetEffectHandler().GetRootContext())

		// Wrap user's generic function into raw function for internal use
		wrappedGetFunc := func() (wm.DELETED_RawVarUpdateFunc, bool, error) {
			typedFunc, skip, err := getComputationFunc()
			if err != nil {
				return nil, skip, err
			}

			// Convert VarUpdateFunc[T] to rawVarUpdateFunc
			rawFunc := func(prev shared.RawWatchValue) shared.RawWatchValue {
				// Extract previous value with type assertion
				var prevT T
				if prev.Value != nil {
					var ok bool
					prevT, ok = prev.Value.(T)
					if !ok {
						var zero T
						panic(fmt.Sprintf(
							"WatchCall '%s': type mismatch on prev value - expected %T, got %T",
							varName, zero, prev.Value,
						))
					}
				} else {
					// Use init value if prev.Value is nil
					prevT = init
				}

				// Execute user's typed function
				nextT, err := typedFunc(prevT)

				// Return as RawHershValue (NotUpdated will be false for actual updates)
				return shared.RawWatchValue{
					Value:      any(nextT),
					Error:      err,
					VarName:    varName,
					NotUpdated: false, // Mark as updated
				}
			}

			return rawFunc, skip, nil
		}

		tickHandle := &manager.TickHandle{
			VarName:            varName,
			GetComputationFunc: wrappedGetFunc,
			Tick:               tick,
			CancelFunc:         cancel,
		}

		if err := mgr.RegisterWatch(varName, tickHandle); err != nil {
			cancel() // Clean up context
			panic(shared.NewWatchInitPanic(
				varName,
				"Failed to register watch",
				err,
			))
		}

		// Start watching in background
		go tickWatchLoop(mgr, tickHandle, ctx)

		// Return initial HershValue[T] on first call
		return shared.WatchValue[T]{
			Value:      init,
			VarName:    varName,
			NotUpdated: true,
		}
	}

	// Get current RawHershValue from VarState
	if mgr != nil {
		rawHV, ok := mgr.GetState().VarState.Get(varName)
		if !ok {
			// Not initialized yet - return zero value
			var zero T
			return shared.WatchValue[T]{Value: zero, VarName: varName}
		}

		// Convert RawHershValue to HershValue[T] with type assertion
		var typedVal T
		if rawHV.Value != nil {
			var ok bool
			typedVal, ok = rawHV.Value.(T)
			if !ok {
				var zero T
				panic(fmt.Sprintf(
					"WatchCall '%s': type mismatch on read - expected %T, got %T",
					varName, zero, rawHV.Value,
				))
			}
		}

		return shared.WatchValue[T]{
			Value:      typedVal,
			Error:      rawHV.Error,
			VarName:    varName,
			NotUpdated: rawHV.NotUpdated,
		}
	}

	var zero T
	return shared.WatchValue[T]{Value: zero, VarName: varName}
}

// tickWatchLoop runs the tick-based Watch monitoring loop.
func tickWatchLoop(mgr *manager.Manager, handle *manager.TickHandle, rootCtx context.Context) {
	ticker := time.NewTicker(handle.Tick)
	defer ticker.Stop()

	for {
		select {
		case <-rootCtx.Done():
			return

		case <-ticker.C:
			// Get computation function and signal flag
			varUpdateFunc, skipSignal, err := handle.GetComputationFunc()
			if err != nil {
				// Log error but continue watching
				if logger := mgr.GetLogger(); logger != nil {
					logger.LogWatchError(handle.VarName, manager.ErrorPhaseGetComputeFunc, err)
				}
			}

			// Send VarSig unless user wants to skip
			if !skipSignal && mgr != nil {
				mgr.GetSignals().SendVarSig(&wm.DELETED_VarSig{
					ReceivedTime:                  time.Now(),
					TargetVarName:                 handle.VarName,
					GetComputeFuncErrOrGetChanErr: err,
					SourceType:                    wm.WatchCallType,
					DELETED_VarUpdateFunc:         varUpdateFunc,
					DELETED_ISStateIndependent:    false, // Tick is state-dependent (apply sequentially)
				})
			}
		}
	}
}

// ! Now, this DELETED_WatchFlow won't be used any more
// ! It Will REPLACED by new Func "WatchFlow"
// DELETED_WatchFlow monitors a channel and emits VarSig when values arrive (generic version).
// This is for event-driven reactive programming.
//
// The init parameter provides the initial value before any channel values are received.
//
// Returns the latest HershValue[T] from the channel or the initial value if none received.
func DELETED_WatchFlow[T any](
	init T,
	getChannelFunc func(ctx context.Context) (<-chan shared.FlowValue[T], error),
	varName string,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	mgr := getManagerFromContext(runCtx)
	if mgr == nil {
		panic(shared.NewWatchInitPanic(
			varName,
			"WatchFlow called with invalid ManageContext",
			nil,
		))
	}

	watchRegistry := mgr.GetWatchRegistry()
	_, exists := watchRegistry.Load(varName)

	if !exists {
		// First call - set initial value in VarState
		initialRaw := shared.RawWatchValue{
			Value:      any(init),
			Error:      nil,
			VarName:    varName,
			NotUpdated: true, // Mark as initial value
		}
		mgr.GetState().VarState.Set(varName, initialRaw)

		// Register and start watching
		// Create channel lifecycle context - use Manager's EffectHandler rootCtx
		flowCtx, cancel := context.WithCancel(mgr.GetEffectHandler().GetRootContext())

		// Wrap user's generic channel into raw channel for internal use
		rawGetChanFunc := func(ctx context.Context) (<-chan shared.RawFlowValue, error) {
			typedChan, err := getChannelFunc(ctx)
			if err != nil {
				return nil, err
			}

			// Convert FlowValue[T] channel to RawFlowValue channel
			rawChan := make(chan shared.RawFlowValue, cap(typedChan))
			go func() {
				defer close(rawChan)
				for fv := range typedChan {
					// Convert FlowValue[T] to RawFlowValue
					rawChan <- shared.RawFlowValue{
						V:          any(fv.V),
						E:          fv.E,
						SkipSignal: fv.SkipSignal,
					}
				}
			}()

			return rawChan, nil
		}

		// Try to create channel
		sourceChan, err := rawGetChanFunc(flowCtx)
		if err != nil {
			cancel()
			// Log error (recovery responsibility is separated)
			mgr.GetLogger().LogWatchError(varName, manager.ErrorPhaseGetComputeFunc, err)

			// Register error RawHershValue with VarName
			errorHV := shared.RawWatchValue{Value: nil, Error: err, VarName: varName}
			mgr.GetState().VarState.Set(varName, errorHV)

			mgr.GetSignals().SendVarSig(
				&wm.DELETED_VarSig{
					ReceivedTime:                  time.Now(),
					TargetVarName:                 varName,
					GetComputeFuncErrOrGetChanErr: err,
					SourceType:                    wm.WatchFlowType,
					DELETED_VarUpdateFunc:         nil,
					DELETED_ISStateIndependent:    true,
				})
			// Return error as HershValue[T]
			return shared.WatchValue[T]{
				Value:      init,
				Error:      err,
				VarName:    varName,
				NotUpdated: true,
			}
		}

		flowHandle := &manager.FlowHandle{
			VarName:        varName,
			GetChannelFunc: rawGetChanFunc,
			CancelFunc:     cancel,
		}

		if err := mgr.RegisterWatch(varName, flowHandle); err != nil {
			cancel() // Clean up context
			panic(shared.NewWatchInitPanic(
				varName,
				"Failed to register watch",
				err,
			))
		}

		// Start watching channel
		go flowWatchLoop(mgr, flowHandle, flowCtx, sourceChan)

		// Return initial HershValue[T]
		return shared.WatchValue[T]{
			Value:      init,
			VarName:    varName,
			NotUpdated: true,
		}
	}

	// Get current RawHershValue from VarState
	if mgr != nil {
		rawHV, ok := mgr.GetState().VarState.Get(varName)
		if !ok {
			var zero T
			return shared.WatchValue[T]{Value: zero, VarName: varName}
		}

		// Convert RawHershValue to HershValue[T] with type assertion
		var typedVal T
		if rawHV.Value != nil {
			var ok bool
			typedVal, ok = rawHV.Value.(T)
			if !ok {
				var zero T
				panic(fmt.Sprintf(
					"WatchFlow '%s': type mismatch on read - expected %T, got %T",
					varName, zero, rawHV.Value,
				))
			}
		}

		return shared.WatchValue[T]{
			Value:      typedVal,
			Error:      rawHV.Error,
			VarName:    varName,
			NotUpdated: rawHV.NotUpdated,
		}
	}

	var zero T
	return shared.WatchValue[T]{Value: zero, VarName: varName}
}

// flowWatchLoop monitors a channel and sends VarSig on updates.
// Now propagates errors to user via RawHershValue instead of skipping them.
func flowWatchLoop(mgr *manager.Manager, handle *manager.FlowHandle, ctx context.Context, sourceChan <-chan shared.RawFlowValue) {
	for {
		select {
		case <-ctx.Done():
			msg := "FlowWatch stopped: " + handle.VarName
			mgr.GetLogger().LogEffect(msg)
			return

		case flowValue, ok := <-sourceChan:
			if !ok {
				// Channel closed
				msg := "Channel closed: " + handle.VarName
				mgr.GetLogger().LogEffect(msg)
				return
			}

			// Send signal unless SkipSignal is true
			if !flowValue.SkipSignal {
				// Wrap value or error in a RawVarUpdateFunc that returns RawHershValue
				varUpdateFunc := func(prev shared.RawWatchValue) shared.RawWatchValue {
					if flowValue.E != nil {
						// Log error but still propagate to user
						mgr.GetLogger().LogWatchError(handle.VarName, manager.ErrorPhaseExecuteComputeFunc, flowValue.E)
						// Return RawHershValue with error
						return shared.RawWatchValue{
							Value:      nil,
							Error:      flowValue.E,
							VarName:    handle.VarName,
							NotUpdated: false, // Error is still an update
						}
					}
					// Return RawHershValue with value
					return shared.RawWatchValue{
						Value:      flowValue.V,
						Error:      nil,
						VarName:    handle.VarName,
						NotUpdated: false, // Flow value is an update
					}
				}

				// Send VarSig
				if mgr != nil {
					mgr.GetSignals().SendVarSig(&wm.DELETED_VarSig{
						ReceivedTime:                  time.Now(),
						GetComputeFuncErrOrGetChanErr: nil,
						SourceType:                    wm.WatchFlowType,
						TargetVarName:                 handle.VarName,
						DELETED_VarUpdateFunc:         varUpdateFunc,
						DELETED_ISStateIndependent:    true, // Flow is state-independent (use last value only)
					})
				}
			}
		}
	}
}
