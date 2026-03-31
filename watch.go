package watch

import (
	"time"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// getManagerFromContext extracts the Manager from ManageContext.
func getManagerFromContext(ctx shared.ManageContext) *manager.Manager {
	if mc, ok := ctx.(*manager.ManageContext); ok {
		return mc.GetManager()
	}
	return nil
}

// WatchCall monitors a value via tick-based polling using WatchMachine.
// MFunc 내에서 호출. WM이 없으면 생성+등록+구독+Start.
// VarState에 값이 없으면 init 반환.
func WatchCall[T any](
	init T,
	setupGetUpdateFunc wm.SetupGetUpdateFunc[T],
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	mgr := getManagerFromContext(runCtx)

	if mgr == nil {
		return shared.RawToTyped[T](shared.RawWatchValueWithName{
			RawWatchValue: shared.RawWatchValue{
				Value: init, NotUpdated: true,
			}, VarName: varName})
	}

	registry := mgr.GetMachineRegistry()
	if registry == nil {
		return shared.RawToTyped[T](shared.RawWatchValueWithName{
			RawWatchValue: shared.RawWatchValue{
				Value: init, NotUpdated: true,
			}, VarName: varName})
	}

	// WM이 없으면 생성
	if _, exists := registry.GetWatchMachine(varName); !exists {
		getRawCallHandle := parseGetRawCallHandle(setupGetUpdateFunc, varName, tick)
		machine := wm.NewWatchMachine(wm.WatchMachineConfig{
			VarName:               varName,
			WatchType:             wm.WatchCallType,
			GetRawCallHandleOrNil: getRawCallHandle,
			LoopCtxConfig:         wm.LoopContextConfig{RunContextTimeout: 1 * time.Minute},
		})
		registry.RegisterWatchMachine(mgr.GetName(), machine)
		machine.RegisterSubscriber(mgr)
		machine.Start()
	}

	// VarState에서 읽기
	return readVarStateOrInit[T](mgr, varName, init)
}

// WatchFlow monitors a value via channel-based flow using WatchMachine.
func WatchFlow[T any](
	init T,
	setupUpdateFuncChan wm.SetupUpdateFuncChan[T],
	varName string,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	mgr := getManagerFromContext(runCtx)
	if mgr == nil {
		return shared.RawToTyped[T](shared.RawWatchValueWithName{
			RawWatchValue: shared.RawWatchValue{
				Value: init, NotUpdated: true,
			}, VarName: varName})
	}

	registry := mgr.GetMachineRegistry()
	if registry == nil {
		return shared.RawToTyped[T](shared.RawWatchValueWithName{
			RawWatchValue: shared.RawWatchValue{
				Value: init, NotUpdated: true,
			}, VarName: varName})
	}

	if _, exists := registry.GetWatchMachine(varName); !exists {
		getRawFlowHandle := parseGetRawFlowHandle(setupUpdateFuncChan, varName)
		machine := wm.NewWatchMachine(wm.WatchMachineConfig{
			VarName:               varName,
			WatchType:             wm.WatchFlowType,
			GetRawFlowHandleOrNil: getRawFlowHandle,
			LoopCtxConfig:         wm.LoopContextConfig{RunContextTimeout: 1 * time.Minute},
		})
		registry.RegisterWatchMachine(mgr.GetName(), machine)
		machine.RegisterSubscriber(mgr)
		machine.Start()
	}

	return readVarStateOrInit[T](mgr, varName, init)
}

// readVarStateOrInit reads from VarState or returns init if not yet available.
func readVarStateOrInit[T any](mgr *manager.Manager, varName string, init T) shared.WatchValue[T] {
	rawVal, ok := mgr.GetManagerState().VarState.Get(varName)

	if !ok || rawVal.Value == nil {
		return shared.RawToTyped[T](shared.RawWatchValueWithName{
			RawWatchValue: shared.RawWatchValue{
				Value: init, NotUpdated: true,
			}, VarName: varName})
	}

	return shared.RawToTyped[T](shared.RawWatchValueWithName{
		RawWatchValue: rawVal,
		VarName:       varName,
	})
}

func parseGetRawCallHandle[T any](
	setupGetUpdateFunc wm.SetupGetUpdateFunc[T],
	varName string,
	tick time.Duration,
) wm.GetRawCallHandleFunc {
	if setupGetUpdateFunc == nil {
		return nil
	}
	return func(callCtx wm.CallContext) (wm.RawCallHandle, error) {
		getUpdateFunc, err := setupGetUpdateFunc(callCtx)
		if err != nil {
			return wm.RawCallHandle{}, err
		}

		return wm.RawCallHandle{
			VarName: varName,
			Tick:    tick,
			NewRawUpdateFunc: func(runCtx wm.RunContext) wm.RawUpdateFunc {
				typedFn := getUpdateFunc(runCtx)
				if typedFn == nil {
					return nil
				}
				return func(prev shared.RawWatchValue) (shared.RawWatchValue, bool) {
					prevTyped := shared.RawToTyped[T](shared.RawWatchValueWithName{
						RawWatchValue: prev,
						VarName:       varName,
					})
					nextTyped, skip := typedFn(prevTyped)
					if skip {
						return prev, true
					}
					return nextTyped.ToRaw(), false
				}
			},
		}, nil
	}
}

func parseGetRawFlowHandle[T any](
	setupUpdateFuncChan wm.SetupUpdateFuncChan[T],
	varName string,
) wm.GetRawFlowHandleFunc {
	if setupUpdateFuncChan == nil {
		return nil
	}
	return func(flowCtx wm.FlowContext) (wm.RawFlowHandle, error) {
		typedChan, err := setupUpdateFuncChan(flowCtx)
		if err != nil {
			return wm.RawFlowHandle{}, err
		}

		rawCh := make(chan wm.RawUpdateFunc, cap(typedChan))

		// 브릿지 고루틴: typed chan → raw chan
		go func() {
			defer close(rawCh)
			for typedFn := range typedChan {
				fn := typedFn // capture
				rawFn := func(prev shared.RawWatchValue) (shared.RawWatchValue, bool) {
					prevTyped := shared.RawToTyped[T](
						shared.RawWatchValueWithName{
							RawWatchValue: prev,
							VarName:       varName,
						},
					)
					nextTyped, skip := fn(prevTyped)
					if skip {
						return prev, true
					}
					return nextTyped.ToRaw(), false
				}
				rawCh <- rawFn
			}
		}()

		return wm.RawFlowHandle{
			VarName:     varName,
			RawFlowChan: rawCh,
		}, nil
	}
}
