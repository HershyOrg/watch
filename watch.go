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
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	registry := mgr.GetMachineRegistry()
	if registry == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
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
		machine.Subscribe(mgr, mgr.GetSignals().NewSigAppended)
		machine.Start()
	}

	// VarState에서 읽기
	return readVarStateOrInit[T](mgr, varName, init)
}

// WatchFlow monitors a value via channel-based flow using WatchMachine.
func WatchFlow[T any](
	init T,
	getFlowHandleFunc wm.GetFlowChan[T],
	varName string,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	mgr := getManagerFromContext(runCtx)
	if mgr == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	registry := mgr.GetMachineRegistry()
	if registry == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	if _, exists := registry.GetWatchMachine(varName); !exists {
		getRawFlowHandle := parseGetRawFlowHandle(getFlowHandleFunc, varName)
		machine := wm.NewWatchMachine(wm.WatchMachineConfig{
			VarName:               varName,
			WatchType:             wm.WatchFlowType,
			GetRawFlowHandleOrNil: getRawFlowHandle,
			LoopCtxConfig:         wm.LoopContextConfig{RunContextTimeout: 1 * time.Minute},
		})
		registry.RegisterWatchMachine(mgr.GetName(), machine)
		machine.Subscribe(mgr, mgr.GetSignals().NewSigAppended)
		machine.Start()
	}

	return readVarStateOrInit[T](mgr, varName, init)
}

// readVarStateOrInit reads from VarState or returns init if not yet available.
func readVarStateOrInit[T any](mgr *manager.Manager, varName string, init T) shared.WatchValue[T] {
	rawVal, ok := mgr.GetManagerState().VarState.Get(varName)
	if !ok || rawVal.Value == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	val := rawVal.Value.(T) // 미스매치 시 panic
	return shared.WatchValue[T]{Value: val, Error: rawVal.Error, VarName: varName}
}

// --- 변환 헬퍼 ---

// rawToTyped converts RawWatchValue to WatchValue[T].
// prev.Value가 nil이면 T의 zero value 사용 (초기 호출).
// 타입 미스매치 시 panic.
func rawToTyped[T any](prev shared.RawWatchValue, varName string) shared.WatchValue[T] {
	if prev.Value == nil {
		var zero T
		return shared.WatchValue[T]{Value: zero, Error: prev.Error, VarName: varName, NotUpdated: true}
	}
	return shared.WatchValue[T]{
		Value:   prev.Value.(T), // 미스매치 시 panic
		Error:   prev.Error,
		VarName: varName,
	}
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
					prevTyped := rawToTyped[T](prev, varName)
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
	getFlowChan wm.GetFlowChan[T],
	varName string,
) wm.GetRawFlowHandleFunc {
	if getFlowChan == nil {
		return nil
	}
	return func(flowCtx wm.FlowContext) (wm.RawFlowHandle, error) {
		typedChan, err := getFlowChan(flowCtx)
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
					prevTyped := rawToTyped[T](prev, varName)
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
