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

// getRegistryFromContext extracts the MachineRegistry from ManageContext.
func getRegistryFromContext(ctx shared.ManageContext) wm.MachineRegistry {
	raw := ctx.GetMachineRegistry()
	if raw == nil {
		return nil
	}
	if reg, ok := raw.(wm.MachineRegistry); ok {
		return reg
	}
	return nil
}

// DELELTED_WatchCall monitors a value via tick-based polling using WatchMachine.
// MFunc 내에서 호출. WM이 없으면 생성+등록+구독+Start.
// VarState에 값이 없으면 init 반환.
func DELELTED_WatchCall[T any](
	init T,
	getCallHandle wm.GetCallHandleFunc[T],
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	mgr := getManagerFromContext(runCtx)
	if mgr == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	registry := getRegistryFromContext(runCtx)
	if registry == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	// WM이 없으면 생성
	if _, exists := registry.GetWatchMachine(varName); !exists {
		rawGetHandle := convertCallHandleToRaw(getCallHandle, varName, tick)
		machine := wm.NewWatchMachine(wm.WatchMachineConfig{
			VarName:               varName,
			WatchType:             wm.WatchCallType,
			GetRawCallHandleOrNil: rawGetHandle,
			LoopCtxConfig:         wm.LoopContextConfig{RunContextTimeout: 1 * time.Minute},
		})
		registry.RegisterWatchMachine(mgr.GetName(), machine)
		machine.Subscribe(mgr, mgr.GetSignals().NewSigAppended)
		machine.Start()
	}

	// VarState에서 읽기
	return readVarStateOrInit[T](mgr, varName, init)
}

// DELETED_WatchFlow monitors a value via channel-based flow using WatchMachine.
func DELETED_WatchFlow[T any](
	init T,
	getFlowHandleFunc wm.GetFlowHandleFunc[T],
	varName string,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	mgr := getManagerFromContext(runCtx)
	if mgr == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	registry := getRegistryFromContext(runCtx)
	if registry == nil {
		return shared.WatchValue[T]{Value: init, NotUpdated: true, VarName: varName}
	}

	if _, exists := registry.GetWatchMachine(varName); !exists {
		rawGetHandle := convertFlowHandleToRaw(getFlowHandleFunc, varName)
		machine := wm.NewWatchMachine(wm.WatchMachineConfig{
			VarName:               varName,
			WatchType:             wm.WatchFlowType,
			GetRawFlowHandleOrNil: rawGetHandle,
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

func convertCallHandleToRaw[T any](
	getCallHandle wm.GetCallHandleFunc[T],
	varName string,
	tick time.Duration,
) wm.GetRawCallHandleFunc {
	if getCallHandle == nil {
		return nil
	}
	return func(callCtx wm.CallContext) (wm.RawCallHandle, error) {
		typedHandle, err := getCallHandle(callCtx)
		if err != nil {
			return wm.RawCallHandle{}, err
		}

		effectiveTick := typedHandle.Tick
		if effectiveTick == 0 {
			effectiveTick = tick
		}

		return wm.RawCallHandle{
			Tick: effectiveTick,
			GetRawUpdateFunc: func(runCtx wm.RunContext) wm.RawUpdateFunc {
				typedFn := typedHandle.GetUpdateFunc(runCtx)
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

func convertFlowHandleToRaw[T any](
	getFlowHandle wm.GetFlowHandleFunc[T],
	varName string,
) wm.GetRawFlowHandleFunc {
	if getFlowHandle == nil {
		return nil
	}
	return func(flowCtx wm.FlowContext) (wm.RawFlowHandle, error) {
		typedHandle, err := getFlowHandle(flowCtx)
		if err != nil {
			return wm.RawFlowHandle{}, err
		}

		rawCh := make(chan wm.RawUpdateFunc, cap(typedHandle.FlowChan))

		// 브릿지 고루틴: typed chan → raw chan
		go func() {
			defer close(rawCh)
			for typedFn := range typedHandle.FlowChan {
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
			RawFlowChan: rawCh,
		}, nil
	}
}
