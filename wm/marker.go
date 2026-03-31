package wm

import "sync"

// Marker는 구독자별 최종 읽은 인덱스를 기록한다.
type Marker struct {
	mu       sync.RWMutex
	lastRead map[string]uint64 // subscriberName -> lastReadIndex
}

// NewMarker는 새 Marker를 생성한다.
func NewMarker() *Marker {
	return &Marker{
		lastRead: make(map[string]uint64),
	}
}

// Register는 구독자의 lastRead를 0으로 초기화한다.
func (m *Marker) Register(subscriberName string) {
	m.mu.Lock()
	m.lastRead[subscriberName] = 0
	m.mu.Unlock()
}

// CheckAndMark는 구독자가 이미 최신인지 확인한다.
// 이미 읽었으면 true(alreadyRead), 안 읽었으면 마킹 후 false를 반환한다.
func (m *Marker) CheckAndMark(subscriberName string, currentIndex uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	lastRead := m.lastRead[subscriberName]
	if lastRead >= currentIndex {
		return true
	}
	m.lastRead[subscriberName] = currentIndex
	return false
}
