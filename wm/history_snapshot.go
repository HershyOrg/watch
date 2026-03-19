package wm

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/HershyOrg/watch/shared"
)

// LoopHistoryConfig는 LoopHistory 순환큐의 설정임.
type LoopHistoryConfig struct {
	MaxLen int           // 최대 보관 개수
	MaxDur time.Duration // 최대 보관 기간
}

// LoopHistory는 병렬안전 순환큐로 구현된 관측 기록임.
// MaxLen과 MaxDur의 교집합으로 윈도잉함.
// * LoopHistory는 State가 아니다. Loop의 관측에 의해 Reduce없이도 계속 append됨.
// * 다만 그 자체로 (이론상) 과거 불변의 로그 with TimeStamp므로,
// * State와 같은 층위의 신뢰도를 지닌다.
type LoopHistory struct {
	mu          sync.RWMutex
	buf         []ReducedSnapshot
	head        int // 가장 오래된 항목의 인덱스
	count       int // 현재 유효 항목 수
	cap         int // buf 크기 (MaxLen)
	maxDur      time.Duration
	appendIndex atomic.Uint64 // Append될 때마다 단조증가하는 인덱스
}

// NewLoopHistory는 LoopHistory를 생성함.
func NewLoopHistory(cfg LoopHistoryConfig) *LoopHistory {
	if cfg.MaxLen <= 0 {
		cfg.MaxLen = 50000
	}
	if cfg.MaxDur <= 0 {
		cfg.MaxDur = 24 * time.Hour
	}
	return &LoopHistory{
		buf:    make([]ReducedSnapshot, cfg.MaxLen),
		cap:    cfg.MaxLen,
		maxDur: cfg.MaxDur,
	}
}

// Append는 snapshot을 순환큐에 추가함.
// MaxLen 초과 시 가장 오래된 항목을 덮어씀.
// MaxDur 초과 항목도 head 전진으로 제거.
func (h *LoopHistory) Append(snapshot ReducedSnapshot) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 순환 방식으로 추가
	idx := (h.head + h.count) % h.cap
	h.buf[idx] = snapshot

	if h.count < h.cap {
		h.count++
	} else {
		// 꽉 참 → head 전진 (가장 오래된 항목 덮어씀)
		h.head = (h.head + 1) % h.cap
	}

	// MaxDur 초과 항목 제거
	h.evictExpired()

	h.appendIndex.Add(1)
}

// evictExpired는 head부터 MaxDur 초과 항목을 제거함. mu 잠금 상태에서 호출.
func (h *LoopHistory) evictExpired() {
	now := time.Now()
	for h.count > 0 {
		oldest := h.buf[h.head]
		if now.Sub(oldest.ReceivedTime) <= h.maxDur {
			break
		}
		h.buf[h.head] = ReducedSnapshot{} // GC 도움
		h.head = (h.head + 1) % h.cap
		h.count--
	}
}

// Last는 가장 최근 snapshot을 반환함.
func (h *LoopHistory) Last() (ReducedSnapshot, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.count == 0 {
		return ReducedSnapshot{}, false
	}
	idx := (h.head + h.count - 1) % h.cap
	return h.buf[idx], true
}

// LastValue는 가장 최근 ReturnedValue를 반환함.
func (h *LoopHistory) LastValue() shared.RawWatchValue {
	snap, ok := h.Last()
	if !ok {
		return shared.RawWatchValue{}
	}
	return snap.ReturnedValue
}

// All은 현재 유효한 모든 snapshot을 시간순 복사본으로 반환함.
func (h *LoopHistory) All() []ReducedSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]ReducedSnapshot, h.count)
	for i := 0; i < h.count; i++ {
		result[i] = h.buf[(h.head+i)%h.cap]
	}
	return result
}

// Len은 현재 유효한 항목 수를 반환함.
func (h *LoopHistory) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}

// AppendIndex는 현재까지의 Append 횟수(단조증가 인덱스)를 반환함.
func (h *LoopHistory) AppendIndex() uint64 {
	return h.appendIndex.Load()
}

// ConsecutiveErrors는 LoopHistory 끝에서부터 연속된 에러 수를 반환함.
// ReturnedValue.Error != nil 또는 GetHandleErrOrNil != nil이면 에러로 간주.
// 정상 값을 만나면 중단.
func (h *LoopHistory) ConsecutiveErrors() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	count := 0
	for i := h.count - 1; i >= 0; i-- {
		snap := h.buf[(h.head+i)%h.cap]
		if snap.ReturnedValue.Error != nil || snap.GetHandleErrOrNil != nil {
			count++
		} else {
			break
		}
	}
	return count
}

// ReducedSnapshot은 그 시점에서 Loop가 변수의 UpdateFunc를 관측 후 적용한 결과다.
type ReducedSnapshot struct {
	ReceivedTime time.Time
	//GetHandleErrOrNil은 GetHandle시의 Err도 포함한다.
	// GetHandle자체에서 Err가 나면 UpdateFunc와 Value는 정해지지 않는다.
	GetHandleErrOrNil error
	//RawUpdateFunc는 Loop가 받은 함수를 저장한다.
	//함수가 달라질 수 있으므로, 반드시 기록하고, 호출해야 한다.
	RawUpdateFunc RawUpdateFunc
	//ReturnedValue는 이전 값에 UpdateFunc를 적용한 결과이다.
	ReturnedValue shared.RawWatchValue
}
