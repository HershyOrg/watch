package watch

import (
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

//go:noinline
func watchRegistrationPC() uintptr {
	const watchRegistrationSkip = 3

	var pcs [1]uintptr
	if runtime.Callers(watchRegistrationSkip, pcs[:]) == 0 {
		return 0
	}
	return pcs[0]
}

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
//
//go:noinline
func WatchCall[T any](
	init T,
	setupGetUpdateFunc wm.SetupGetUpdateFunc[T],
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	contract := watchContract[T](varName, wm.WatchCallType, watchRegistrationPC())
	return watchCallWithContract(init, setupGetUpdateFunc, varName, tick, runCtx, contract)
}

func watchCallWithContract[T any](
	init T,
	setupGetUpdateFunc wm.SetupGetUpdateFunc[T],
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
	contract wm.WatchContract,
) shared.WatchValue[T] {
	if tick <= 0 {
		panicWatchInit(varName, fmt.Sprintf("%s tick must be positive (got %s)", contract.WatchType, tick))
	}

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
	if existing, exists := registry.GetWatchMachine(varName); exists {
		validateWatchContract(varName, existing.Contract, contract)
	} else {
		getRawCallHandle := parseGetRawCallHandle(setupGetUpdateFunc, varName, tick)
		cfg := mgr.GetConfig()
		machine := wm.NewWatchMachine(wm.WatchMachineConfig{
			VarName:               varName,
			WatchType:             contract.WatchType,
			Contract:              contract,
			GetRawCallHandleOrNil: getRawCallHandle,
			LoopCtxConfig:         wm.LoopContextConfig{RunContextTimeout: 1 * time.Minute},
			HistoryMaxLen:         cfg.WatchMachineHistoryMaxLen,
			HistoryMaxDur:         cfg.WatchMachineHistoryMaxDur,
		})
		if err := registry.RegisterWatchMachine(mgr.GetName(), machine); err != nil {
			panicWatchInit(varName, fmt.Sprintf("failed to register watch machine: %v", err))
		}
		machine.RegisterSubscriber(mgr)
		if err := machine.Start(); err != nil {
			panicWatchInit(varName, fmt.Sprintf("failed to start watch machine: %v", err))
		}
	}

	// VarState에서 읽기
	return readVarStateOrInit(mgr, varName, init)
}

// WatchFlow monitors a value via channel-based flow using WatchMachine.
//
//go:noinline
func WatchFlow[T any](
	init T,
	setupUpdateFuncChan wm.SetupUpdateFuncChan[T],
	varName string,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	contract := watchContract[T](varName, wm.WatchFlowType, watchRegistrationPC())

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

	if existing, exists := registry.GetWatchMachine(varName); exists {
		validateWatchContract(varName, existing.Contract, contract)
	} else {
		getRawFlowHandle := parseGetRawFlowHandle(setupUpdateFuncChan, varName)
		cfg := mgr.GetConfig()
		machine := wm.NewWatchMachine(wm.WatchMachineConfig{
			VarName:               varName,
			WatchType:             wm.WatchFlowType,
			Contract:              contract,
			GetRawFlowHandleOrNil: getRawFlowHandle,
			LoopCtxConfig:         wm.LoopContextConfig{RunContextTimeout: 1 * time.Minute},
			HistoryMaxLen:         cfg.WatchMachineHistoryMaxLen,
			HistoryMaxDur:         cfg.WatchMachineHistoryMaxDur,
		})
		if err := registry.RegisterWatchMachine(mgr.GetName(), machine); err != nil {
			panicWatchInit(varName, fmt.Sprintf("failed to register watch machine: %v", err))
		}
		machine.RegisterSubscriber(mgr)
		if err := machine.Start(); err != nil {
			panicWatchInit(varName, fmt.Sprintf("failed to start watch machine: %v", err))
		}
	}

	return readVarStateOrInit(mgr, varName, init)
}

func watchContract[T any](varName string, watchType wm.WatchType, registrationPC uintptr) wm.WatchContract {
	return wm.WatchContract{
		VarName:        varName,
		WatchType:      watchType,
		ValueType:      reflect.TypeFor[T](),
		RegistrationPC: registrationPC,
	}
}

func validateWatchContract(varName string, existing, next wm.WatchContract) {
	if existing.VarName == "" {
		return
	}
	if existing.VarName == next.VarName &&
		existing.WatchType == next.WatchType &&
		existing.ValueType == next.ValueType &&
		existing.RegistrationPC == next.RegistrationPC {
		return
	}

	panicWatchInit(varName, fmt.Sprintf(
		"watch registration conflict: existing={type:%s value:%v pc:%#x} next={type:%s value:%v pc:%#x}",
		existing.WatchType,
		existing.ValueType,
		existing.RegistrationPC,
		next.WatchType,
		next.ValueType,
		next.RegistrationPC,
	))
}

func panicWatchInit(varName, reason string) {
	panic(shared.NewWatchInitPanic(varName, reason, nil))
}

// readVarStateOrInit reads from VarState or returns init if not yet available.
func readVarStateOrInit[T any](mgr *manager.Manager, varName string, init T) shared.WatchValue[T] {
	rawVal, ok := mgr.GetManagerState().VarState.Get(varName)

	if !ok || (rawVal.Value == nil && rawVal.Error == nil) {
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
		if typedChan == nil {
			return wm.RawFlowHandle{}, fmt.Errorf("WatchFlow setup returned nil channel for var %q", varName)
		}

		rawCh := make(chan wm.RawUpdateFunc, cap(typedChan))

		// 브릿지 고루틴: typed chan → raw chan
		go func() {
			defer close(rawCh)
			for {
				var typedFn wm.UpdateFunc[T]
				select {
				case <-flowCtx.Done():
					return
				case fn, ok := <-typedChan:
					if !ok {
						return
					}
					typedFn = fn
				}

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

				select {
				case rawCh <- rawFn:
				case <-flowCtx.Done():
					return
				}
			}
		}()

		return wm.RawFlowHandle{
			VarName:     varName,
			RawFlowChan: rawCh,
		}, nil
	}
}
