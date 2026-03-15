package wm

import (
	"time"

	"github.com/HershyOrg/watch/shared"
)

// LoopEvent는 WatchMachine상에서 발생하는,
// Loop를 위한 이벤트임.
type LoopEvent interface {
	LoopEvent()
}

// LoopWatchedNewVar는 UpdateFunc적용 후 WatchLoop가 발견한 새 에러이다.
// 값을 Watch후, 에러가 발생 시 이를 통해 보고한다.
type LoopGotErrFromUpdateFunc struct {
	WatchedTime   time.Time
	RawUpdateFunc RawUpdateFunc
	Err           error
}

func (lw *LoopGotErrFromUpdateFunc) LoopEvent(){}

// LoopGotErrFromGetHandle는 Loop가 Handle을 얻지 못한 사건이다.
type LoopGotErrFromGetHandle struct {
	WatchType                 WatchType
	GetRawCallHandleFuncOrNil GetRawCallHandleFunc
	GetFlowHandleFuncOrNil    GetRawFlowHandleFunc
}

func (lg *LoopGotErrFromGetHandle) LoopEvent(){}

// WmCheckedAllSubscribers 는 WatchMachine이 자신의 구독자들 상태를
// GC루틴으로 체크한 사건이다.
// 구독자들의 상태를 보고한다.
type WmCheckedAllSubscribers struct {
	RecievedTime         time.Time
	SubscribersWithState []SubscriberWithState
}

func (wc *WmCheckedAllSubscribers) LoopEvent(){}

// WmCheckedAllPublishers는 WatchMachine이 자신의 발행자들 상태를
// PbChecker루틴으로 체크한 사건이다.
// 발행자들의 상태를 보고한다.
type WmCheckedAllPublishers struct {
	RecievedTime        time.Time
	PublishersWithState []PublisherWithState
}

func (wc *WmCheckedAllPublishers) LoopEvent(){}

// SubscriberWithState는 구독자와 그 상태이다.
type SubscriberWithState struct {
	CheckedTime time.Time
	State       shared.ManagerInnerState
	Subscriber  Subscriber
}

// SubscriberWithState는 발행자와 그 상태이다.
type PublisherWithState struct {
	CheckedTime time.Time
	State       shared.ManagerInnerState
	Subscriber  Publisher
}
