package wm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/HershyOrg/watch/shared"
)

// WatchLoop는 WatchLoopInterface의 구체 구현임.
// Manager에서의 MgrFuncRunner(=Target)와 동일한 역할.
// Effect를 받아 실행하고, 결과를 LoopEffectDrivenEvent로 반환함.
type WatchLoop struct {
	watchType             WatchType
	getRawCallHandleOrNil GetRawCallHandleFunc
	getRawFlowHandleOrNil GetRawFlowHandleFunc
	loopCtxConfig         LoopContextConfig
	loopHistory           *LoopHistory
	notifyChs             *[]chan struct{}
	varName               string
	recoveryPolicy        LoopRecoveryPolicy
	eventChan             chan<- LoopEvent // WatchMachine의 eventChan 참조

	// 런타임 상태
	rootMu     sync.Mutex
	rootCancel context.CancelFunc
}

// NewWatchLoop는 WatchLoop를 생성함.
func NewWatchLoop(
	varName string,
	watchType WatchType,
	getRawCallHandleOrNil GetRawCallHandleFunc,
	getRawFlowHandleOrNil GetRawFlowHandleFunc,
	loopCtxConfig LoopContextConfig,
	loopHistory *LoopHistory,
	notifyChs *[]chan struct{},
	recoveryPolicy LoopRecoveryPolicy,
	eventChan chan<- LoopEvent,
) *WatchLoop {
	return &WatchLoop{
		varName:               varName,
		watchType:             watchType,
		getRawCallHandleOrNil: getRawCallHandleOrNil,
		getRawFlowHandleOrNil: getRawFlowHandleOrNil,
		loopCtxConfig:         loopCtxConfig,
		loopHistory:           loopHistory,
		notifyChs:             notifyChs,
		recoveryPolicy:        recoveryPolicy,
		eventChan:             eventChan,
	}
}

// Execute는 Reducer가 생성한 LoopEffect를 받아 실행하고,
// 그 결과를 LoopEffectDrivenEvent로 반환함.
func (wl *WatchLoop) Execute(effect LoopEffect) LoopEffectDrivenEvent {
	switch effect.(type) {
	case *StartLoop:
		return wl.executeStart()
	case *TryRecoverLoop:
		return wl.executeTryRecover()
	case *StopLoop:
		return wl.executeStop()
	case *KillLoop:
		return wl.executeKill()
	case *CrashLoop:
		return wl.executeCrash()
	default:
		return &LoopCrashCompleted{}
	}
}

func (wl *WatchLoop) executeStart() LoopEffectDrivenEvent {
	ctx, cancel := context.WithCancel(context.Background())
	wl.replaceRootCancel(cancel)

	switch wl.watchType {
	case WatchCallType:
		return wl.startCallLoop(ctx)
	case WatchFlowType:
		return wl.startFlowLoop(ctx)
	default:
		return &LoopStartFailed{Err: fmt.Errorf("unsupported watch type: %s", wl.watchType)}
	}
}

func (wl *WatchLoop) startCallLoop(ctx context.Context) LoopEffectDrivenEvent {
	if wl.getRawCallHandleOrNil == nil {
		err := fmt.Errorf("GetRawCallHandleFunc is nil")
		wl.recordGetHandleErr(err)
		return &LoopGotErrFromGetHandle{
			WatchType: wl.watchType,
			Err:       err,
		}
	}

	// GetHandle 호출 (패닉 핸들링)
	handle, err := wl.safeGetCallHandle(ctx)
	if err != nil {
		wl.recordGetHandleErr(err)
		return &LoopGotErrFromGetHandle{
			WatchType:                 wl.watchType,
			GetRawCallHandleFuncOrNil: wl.getRawCallHandleOrNil,
			Err:                       err,
		}
	}

	// 초기값은 기록하지 않음. Init은 "읽는 측"의 책임.
	// ticker 고루틴 시작
	go wl.runCallTicker(ctx, handle)

	return &LoopStarted{}
}

// recordGetHandleErr는 GetHandle 실패 시 LoopHistory에 에러 snapshot을 기록함.
func (wl *WatchLoop) recordGetHandleErr(err error) {
	errSnapshot := ReducedSnapshot{
		ReceivedTime:      time.Now(),
		GetHandleErrOrNil: err,
		RawUpdateFunc:     nil,
		ReturnedValue: shared.RawWatchValue{
			Error: err,
		},
	}
	wl.loopHistory.Append(errSnapshot)
	wl.notifySubscribers()
}

func (wl *WatchLoop) runCallTicker(ctx context.Context, handle RawCallHandle) {
	ticker := time.NewTicker(handle.Tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			drainDur := wl.processCallTick(ctx, handle)
			if drainDur > 0 {
				// drain 동안 밀린 tick을 버림
				wl.drainTickerChanDuring(ctx, ticker.C, drainDur)
			}
		}
	}
}

// processCallTick는 한 번의 tick을 처리함.
// 에러 처리 통합: GetUpdateFunc 패닉 → 에러 리턴하는 UpdateFunc로 대체.
// UpdateFunc 패닉 → 에러를 담은 RawWatchValue로 처리.
// 모든 경로가 동일한 snapshot 기록 흐름을 따름.
// processCallTick는 한 번의 tick을 처리하고, drain이 필요하면 그 duration을 반환함.
func (wl *WatchLoop) processCallTick(ctx context.Context, handle RawCallHandle) time.Duration {
	// RunContext 생성 (timeout)
	timeout := wl.loopCtxConfig.RunContextTimeout
	if timeout <= 0 {
		timeout = 1 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rc := &simpleRunContext{Context: runCtx}

	// 1) GetRawUpdateFunc 호출 (패닉 핸들링)
	//    패닉 시: 에러를 리턴하는 UpdateFunc로 대체
	updateFunc := wl.safeGetUpdateFunc(handle, rc)

	// 2) UpdateFunc(prev) 호출 (패닉 핸들링)
	//    패닉 시: 에러를 담은 RawWatchValue, skip=false
	prev := wl.loopHistory.LastValue()
	next, skip := wl.safeCallUpdateFunc(updateFunc, prev)

	// 3) skip==true면 아무것도 안 함
	if skip {
		return 0
	}

	// 4) skip==false면: snapshot 기록 → Subscriber 알림
	snapshot := ReducedSnapshot{
		ReceivedTime:  time.Now(),
		RawUpdateFunc: updateFunc,
		ReturnedValue: next,
	}
	wl.loopHistory.Append(snapshot)
	wl.notifySubscribers()

	// 5) 에러 시 리커버리 판단
	if next.Error != nil {
		return wl.handleUpdateFuncError()
	}
	return 0
}

// --- Flow ---

func (wl *WatchLoop) startFlowLoop(ctx context.Context) LoopEffectDrivenEvent {
	if wl.getRawFlowHandleOrNil == nil {
		err := fmt.Errorf("GetRawFlowHandleFunc is nil")
		wl.recordGetHandleErr(err)
		return &LoopGotErrFromGetHandle{
			WatchType: wl.watchType,
			Err:       err,
		}
	}

	handle, err := wl.safeGetFlowHandle(ctx)
	if err != nil {
		wl.recordGetHandleErr(err)
		return &LoopGotErrFromGetHandle{
			WatchType:              wl.watchType,
			GetFlowHandleFuncOrNil: wl.getRawFlowHandleOrNil,
			Err:                    err,
		}
	}

	go wl.runFlowListener(ctx, handle)

	return &LoopStarted{}
}

func (wl *WatchLoop) runFlowListener(ctx context.Context, handle RawFlowHandle) {
	for {
		select {
		case <-ctx.Done():
			return
		case updateFunc, ok := <-handle.RawFlowChan:
			if !ok {
				return // 채널 닫힘
			}
			drainDur := wl.processFlowUpdate(updateFunc)
			if drainDur > 0 {
				wl.drainFlowChanDuring(ctx, handle.RawFlowChan, drainDur)
			}
		}
	}
}

// processFlowUpdate는 Flow에서 수신한 UpdateFunc를 처리함.
// drain이 필요하면 그 duration을 반환함 (caller가 채널 drain).
func (wl *WatchLoop) processFlowUpdate(updateFunc RawUpdateFunc) time.Duration {
	prev := wl.loopHistory.LastValue()
	next, skip := wl.safeCallUpdateFunc(updateFunc, prev)

	if skip {
		return 0
	}

	snapshot := ReducedSnapshot{
		ReceivedTime:  time.Now(),
		RawUpdateFunc: updateFunc,
		ReturnedValue: next,
	}
	wl.loopHistory.Append(snapshot)
	wl.notifySubscribers()

	if next.Error != nil {
		return wl.handleUpdateFuncError()
	}
	return 0
}

func (wl *WatchLoop) safeGetFlowHandle(ctx context.Context) (handle RawFlowHandle, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in GetFlowHandle: %v", r)
		}
	}()

	fc := &simpleRunContext{Context: ctx}
	handle, err = wl.getRawFlowHandleOrNil(fc)
	return
}

// --- Recovery ---

func (wl *WatchLoop) executeTryRecover() LoopEffectDrivenEvent {
	// sleep 전에 반드시 Loop 중지 — 어떤 경로에서든
	// History 누적 방지
	wl.cancelRoot()

	consecutiveErrs := wl.loopHistory.ConsecutiveErrors()
	if consecutiveErrs >= wl.recoveryPolicy.MaxConsecutiveFailures {
		return &LoopRecoveryCrashed{}
	}
	delay := wl.recoveryPolicy.CalculateBackoff(consecutiveErrs)
	time.Sleep(delay)
	return &LoopRecoveryApplied{}
}

// handleUpdateFuncError는 UpdateFunc 에러 발생 시 리커버리 판단을 수행함.
// 반환값: caller가 drain해야 할 duration. caller(runCallTicker/runFlowListener)가 자신의 소스를 drain.
// Tier 1: delay 동안 들어오는 모든 신호를 버림
// Tier 2: eventChan에 RecoveryRequested 삽입 (전체 복구)
func (wl *WatchLoop) handleUpdateFuncError() time.Duration {
	consecutiveErrs := wl.loopHistory.ConsecutiveErrors()

	if consecutiveErrs >= wl.recoveryPolicy.MinConsecutiveFailures {
		select {
		case wl.eventChan <- &RecoveryRequested{}:
		default:
		}
		return 0
	}

	return wl.recoveryPolicy.LightweightDelay(consecutiveErrs)
}

// drainTickerChanDuring은 dur 동안 ticker 채널에서 들어오는 tick을 버림.
// runCallTicker에서 직접 호출됨 (같은 고루틴이므로 경쟁 없음).
func (wl *WatchLoop) drainTickerChanDuring(ctx context.Context, tickerC <-chan time.Time, dur time.Duration) {
	timer := time.NewTimer(dur)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return
		case <-tickerC:
			// tick 버림
		case <-ctx.Done():
			return
		}
	}
}

func (wl *WatchLoop) drainFlowChanDuring(ctx context.Context, ch <-chan RawUpdateFunc, dur time.Duration) {
	timer := time.NewTimer(dur)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// --- Stop/Kill/Crash ---

func (wl *WatchLoop) executeStop() LoopEffectDrivenEvent {
	wl.cancelRoot()
	return &LoopStopCompleted{}
}

func (wl *WatchLoop) executeKill() LoopEffectDrivenEvent {
	wl.cancelRoot()
	return &LoopKillCompleted{}
}

func (wl *WatchLoop) executeCrash() LoopEffectDrivenEvent {
	wl.cancelRoot()
	return &LoopCrashCompleted{}
}

// --- 패닉 핸들링 헬퍼 ---

func (wl *WatchLoop) safeGetCallHandle(ctx context.Context) (handle RawCallHandle, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in GetCallHandle: %v", r)
		}
	}()

	rc := &simpleRunContext{Context: ctx}
	handle, err = wl.getRawCallHandleOrNil(rc)
	return
}

// safeGetUpdateFunc는 GetRawUpdateFunc를 패닉 핸들링하여 호출함.
// 패닉 시, 에러를 리턴하는 UpdateFunc를 만들어 반환함.
// 주석: "UpdateFunc를 얻는 것의 실패는 UpdateFunc의 리턴값에 표현"
func (wl *WatchLoop) safeGetUpdateFunc(handle RawCallHandle, rc RunContext) RawUpdateFunc {
	var fn RawUpdateFunc
	var panicErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = fmt.Errorf("panic in GetRawUpdateFunc: %v", r)
			}
		}()
		fn = handle.NewRawUpdateFunc(rc)
	}()

	if panicErr != nil {
		// 패닉을 에러를 리턴하는 UpdateFunc로 대체
		return func(prev shared.RawWatchValue) (shared.RawWatchValue, bool) {
			return shared.RawWatchValue{
				Error: panicErr,
			}, false
		}
	}

	if fn == nil {
		// nil UpdateFunc → skip하는 UpdateFunc로 대체
		return func(prev shared.RawWatchValue) (shared.RawWatchValue, bool) {
			return prev, true
		}
	}

	return fn
}

// safeCallUpdateFunc는 UpdateFunc를 패닉 핸들링하여 호출함.
// 패닉 시, 에러를 담은 RawWatchValue를 반환하고 skip=false.
func (wl *WatchLoop) safeCallUpdateFunc(fn RawUpdateFunc, prev shared.RawWatchValue) (next shared.RawWatchValue, skip bool) {
	defer func() {
		if r := recover(); r != nil {
			next = shared.RawWatchValue{
				Error: fmt.Errorf("panic in UpdateFunc: %v", r),
			}
			skip = false
		}
	}()

	next, skip = fn(prev)
	return
}

func (wl *WatchLoop) notifySubscribers() {
	if wl.notifyChs == nil {
		return
	}
	for _, ch := range *wl.notifyChs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// simpleRunContext는 RunContext 인터페이스의 최소 구현임.
type simpleRunContext struct {
	context.Context
}

func (s *simpleRunContext) RunContext()  {}
func (s *simpleRunContext) RootContext() {}

func (wl *WatchLoop) replaceRootCancel(cancel context.CancelFunc) {
	wl.rootMu.Lock()
	oldCancel := wl.rootCancel
	wl.rootCancel = cancel
	wl.rootMu.Unlock()

	if oldCancel != nil {
		oldCancel()
	}
}

func (wl *WatchLoop) cancelRoot() {
	wl.rootMu.Lock()
	cancel := wl.rootCancel
	wl.rootCancel = nil
	wl.rootMu.Unlock()

	if cancel != nil {
		cancel()
	}
}
