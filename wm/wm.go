package wm

import (
	"context"
	"reflect"
	"time"

	"github.com/HershyOrg/watch/shared"
)

// WatchMachine은 Watch와 관련한 기능을 한데 모은 구조체임.
// Manager는 WatchMachine을 Subscribe함으로써 새 변수 값을 감지-추적 가능함.
type WatchMachine struct {
	//varName을 통해 WatchMachine을 식별함
	VarName string

	//WatchXXX설정 보관
	WatchType             WatchType
	Contract              WatchContract
	GetRawCallHandleOrNil GetRawCallHandleFunc
	GetRawFlowHandleOrNil GetRawFlowHandleFunc

	//reduce-effect엔진
	loopReducer LoopReducer
	//loop의 상태-조작을 reducer-effect가 담당-지시함.
	//Loop는 Manager의 MgrFuncRunner(=Target)와 동일한 역할임.
	loop *WatchLoop

	//currentLoopState는 Loop의 현재 상태임.
	//Reducer가 순수 함수이므로, WatchMachine이 state를 보관함.
	currentLoopState LoopState

	//loopHistory는 watchMachine의 관측기록을 나타낸다.
	loopHistory *LoopHistory

	//loopCtxConfig로 WatchMachine의 생명주기-타임아웃 결정
	loopCtxConfig LoopContextConfig

	//eventChan은 외부 제어 이벤트(Start/Stop/Kill)를 이벤트 루프에 전달함.
	eventChan chan LoopEvent

	//notifyChs는 Subscriber에게 NewSigAppend 알림을 보내기 위한 쓰기 가능 채널들.
	notifyChs []chan struct{}

	//cancelEventLoop는 이벤트 루프 고루틴을 종료하기 위한 함수.
	cancelEventLoop context.CancelFunc

	//Subscribers는 WatchMachine을 구독한 Manager들임.
	Subscribers []Subscriber

	//PublisherOrNil는 Multi-Manager가 구현되었을 시,
	//해당 WatchMachine을 Export한 Manager를 나타냄
	PublishersOrNil []Publisher

	//MachineRegistry에 WatchMachine을 등록함으로써,
	//Wathcer가 모든 등록된 WatchMachine을 한 곳에서 조회 가능하게 한다.
	MachineRegistry MachineRegistry

	//PublishersChecker를 통해 자신을 Export한 Manager가 죽었는지 체크함.
	PublishersChecker PublishersCheckerInterface

	//marker는 구독자별 최종 읽은 인덱스를 기록한다.
	marker *Marker
}

type LoopContextConfig struct {
	RunContextTimeout  time.Duration
	RootContextTimeout time.Duration
}
type WatchType string

const (
	WatchFlowType WatchType = "WatchFlowType"
	WatchCallType WatchType = "WatchCallType"
)

// WatchContract identifies the setup that owns a varName.
type WatchContract struct {
	VarName        string
	WatchType      WatchType
	ValueType      reflect.Type
	RegistrationPC uintptr
}

// WatchMachineConfig는 WatchMachine 생성에 필요한 설정임.
type WatchMachineConfig struct {
	VarName               string
	WatchType             WatchType
	Contract              WatchContract
	GetRawCallHandleOrNil GetRawCallHandleFunc
	GetRawFlowHandleOrNil GetRawFlowHandleFunc
	LoopCtxConfig         LoopContextConfig
	RecoveryPolicy        LoopRecoveryPolicy
}

// NewWatchMachine은 WatchMachine을 생성하고 이벤트 루프를 시작함.
func NewWatchMachine(cfg WatchMachineConfig) *WatchMachine {
	// RecoveryPolicy 기본값 적용
	if cfg.RecoveryPolicy.MaxConsecutiveFailures == 0 {
		cfg.RecoveryPolicy = DefaultLoopRecoveryPolicy()
	}
	//* 여기 하드코딩된 부분은
	//* 추후 WatchXXX에 윈도우 추가 시 설정값 기반으로 바뀔 예정임.
	history := NewLoopHistory(LoopHistoryConfig{
		varName: cfg.VarName,
		MaxLen:  30000,
		MaxDur:  24 * time.Hour,
	})
	notifyChs := make([]chan struct{}, 0)
	eventChan := make(chan LoopEvent, 100)

	wm := &WatchMachine{
		VarName:               cfg.VarName,
		WatchType:             cfg.WatchType,
		Contract:              cfg.Contract,
		GetRawCallHandleOrNil: cfg.GetRawCallHandleOrNil,
		GetRawFlowHandleOrNil: cfg.GetRawFlowHandleOrNil,
		loopReducer: LoopReducer{
			loopHistory:    history,
			recoveryPolicy: cfg.RecoveryPolicy,
		},
		currentLoopState: &LoopIdle{},
		loopHistory:      history,
		loopCtxConfig:    cfg.LoopCtxConfig,
		eventChan:        eventChan,
		notifyChs:        notifyChs,
		marker:           NewMarker(),
	}

	wm.loop = NewWatchLoop(
		cfg.VarName,
		cfg.WatchType,
		cfg.GetRawCallHandleOrNil,
		cfg.GetRawFlowHandleOrNil,
		cfg.LoopCtxConfig,
		history,
		&notifyChs,
		cfg.RecoveryPolicy,
		eventChan,
	)

	// 이벤트 루프 시작
	ctx, cancel := context.WithCancel(context.Background())
	wm.cancelEventLoop = cancel
	go wm.runEventLoop(ctx)

	return wm
}

// runEventLoop는 eventChan을 감시하며 이벤트를 처리함.
func (wm *WatchMachine) runEventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-wm.eventChan:
			wm.reduceAndExecute(event)
		}
	}
}

// reduceAndExecute는 LoopEvent를 Reduce 후 Effect를 실행하고,
// DrivenEvent를 재귀적으로 처리함.
func (wm *WatchMachine) reduceAndExecute(event LoopEvent) {
	nextState, effects := wm.loopReducer.Reduce(wm.currentLoopState, event)
	wm.currentLoopState = nextState

	for _, effect := range effects {
		driven := wm.loop.Execute(effect)
		if driven != nil {
			wm.reduceAndExecuteDriven(driven)
		}
	}
}

// reduceAndExecuteDriven은 DrivenEvent를 ReduceDriven 후 Effect를 실행하고,
// 재귀적으로 처리함.
func (wm *WatchMachine) reduceAndExecuteDriven(driven LoopEffectDrivenEvent) {
	nextState, effects := wm.loopReducer.ReduceDriven(wm.currentLoopState, driven)
	wm.currentLoopState = nextState

	for _, effect := range effects {
		drivenResult := wm.loop.Execute(effect)
		if drivenResult != nil {
			wm.reduceAndExecuteDriven(drivenResult)
		}
	}
}

// Start는 WatchMachine의 Loop를 시작함.
func (wm *WatchMachine) Start() {
	wm.eventChan <- &StartRequested{NeedInit: true}
}

// Stop은 WatchMachine의 Loop를 정상 종료함.
func (wm *WatchMachine) Stop() {
	wm.eventChan <- &StopRequested{}
}

// Kill은 WatchMachine의 Loop를 강제 종료함.
func (wm *WatchMachine) Kill() {
	wm.eventChan <- &KillRequested{}
}

// GetLoopState는 현재 Loop 상태를 반환함.
func (wm *WatchMachine) GetLoopState() LoopState {
	return wm.currentLoopState
}

// GetLoopHistory는 현재 유효한 관측 기록을 시간순 복사본으로 반환함.
func (wm *WatchMachine) GetLoopHistory() []ReducedSnapshot {
	return wm.loop.loopHistory.All()
}

// Close는 이벤트 루프를 종료함.
func (wm *WatchMachine) Close() {
	if wm.cancelEventLoop != nil {
		wm.cancelEventLoop()
	}
}

// GetLoopHistoryRef는 LoopHistory 참조를 반환함.
func (wm *WatchMachine) GetLoopHistoryRef() *LoopHistory {
	return wm.loopHistory
}

// GetAppendIndex는 현재 appendIndex를 반환함.
func (wm *WatchMachine) GetAppendIndex() uint64 {
	return wm.loopHistory.AppendIndex()
}

// ReadLatestFor는 구독자에게 최신값을 반환한다.
// 이미 읽었으면 (zero, true) — "이미 최신"
// 안 읽었으면 마킹 후 (value, false) — "새 값"
func (wm *WatchMachine) ReadLatestFor(subscriberName string) (shared.RawWatchValue, bool) {
	currentIndex := wm.loopHistory.AppendIndex()
	alreadyRead := wm.marker.CheckAndMark(subscriberName, currentIndex)
	if alreadyRead {
		return shared.RawWatchValue{}, true
	}
	return wm.loopHistory.LastValue(), false
}

// RegisterSubscriber는 구독자를 등록한다.
func (wm *WatchMachine) RegisterSubscriber(sub Subscriber) {
	wm.marker.Register(sub.GetName())
	*wm.loop.notifyChs = append(*wm.loop.notifyChs, sub.GetNewSigAppendedChan())
	wm.Subscribers = append(wm.Subscribers, sub)
}
