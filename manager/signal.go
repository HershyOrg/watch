// Package manager implements the Manager component of the hersh framework.
// Manager handles state management through Reducer and Effect System.
package manager

import (
	"time"

	"github.com/HershyOrg/watch/shared"
)

// SignalChannels holds all event queues for the Manager.
// UserEvent와 ControlEvent는 병렬 안전 큐로 관리되며,
// NewSigAppended 채널은 새 이벤트 도착 알림 전용으로만 사용된다.
type SignalChannels struct {
	userQueue      *shared.SafeQueue[*UserMessageReceived]
	controlQueue   *shared.SafeQueue[*ControlEvent]
	NewSigAppended chan struct{} // 알림 전용 채널
}

// NewSignalChannels creates a new SignalChannels with SafeQueues.
func NewSignalChannels(notifyBufferSize int) *SignalChannels {
	return &SignalChannels{
		userQueue:      shared.NewSafeQueue[*UserMessageReceived](),
		controlQueue:   shared.NewSafeQueue[*ControlEvent](),
		NewSigAppended: make(chan struct{}, notifyBufferSize*3),
	}
}

// SendUserEvent enqueues a UserMessageReceived and notifies.
func (sc *SignalChannels) SendUserEvent(event *UserMessageReceived) {
	sc.userQueue.Enqueue(event)
	select {
	case sc.NewSigAppended <- struct{}{}:
	default:
	}
}

// SendControlEvent enqueues a ControlEvent and notifies.
func (sc *SignalChannels) SendControlEvent(event *ControlEvent) {
	sc.controlQueue.Enqueue(event)
	select {
	case sc.NewSigAppended <- struct{}{}:
	default:
	}
}

// DequeueUserEvent dequeues one UserEvent. Returns (nil, false) if empty.
func (sc *SignalChannels) DequeueUserEvent() (*UserMessageReceived, bool) {
	return sc.userQueue.Dequeue()
}

// DequeueControlEvent dequeues one ControlEvent. Returns (nil, false) if empty.
func (sc *SignalChannels) DequeueControlEvent() (*ControlEvent, bool) {
	return sc.controlQueue.Dequeue()
}

// UserQueueLen returns the current user event queue length.
func (sc *SignalChannels) UserQueueLen() int {
	return sc.userQueue.Len()
}

// ControlQueueLen returns the current control event queue length.
func (sc *SignalChannels) ControlQueueLen() int {
	return sc.controlQueue.Len()
}

// Close closes the notification channel.
func (sc *SignalChannels) Close() {
	close(sc.NewSigAppended)
}

// SignalPeekEntry는 큐에 있는 시그널 정보를 나타낸다 (API 모니터링용).
type SignalPeekEntry struct {
	Type      string
	Content   string
	CreatedAt time.Time
}

// PeekSignals는 큐에 있는 시그널들을 안전하게 peek한다 (비파괴적).
func (sc *SignalChannels) PeekSignals(maxCount int) []SignalPeekEntry {
	entries := []SignalPeekEntry{}

	userEvents := sc.userQueue.Peek(maxCount / 2)
	for _, sig := range userEvents {
		entries = append(entries, SignalPeekEntry{
			Type:      "user",
			Content:   sig.String(),
			CreatedAt: sig.CreatedAt(),
		})
	}

	controlEvents := sc.controlQueue.Peek(maxCount / 2)
	for _, sig := range controlEvents {
		entries = append(entries, SignalPeekEntry{
			Type:      "watcher",
			Content:   sig.String(),
			CreatedAt: sig.CreatedAt(),
		})
	}

	return entries
}
