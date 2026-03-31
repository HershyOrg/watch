package shared

import "sync"

// SafeQueue는 타입 안전한 병렬 안전 FIFO 큐이다.
type SafeQueue[T any] struct {
	mu    sync.Mutex
	items []T
}

// NewSafeQueue는 새 SafeQueue를 생성한다.
func NewSafeQueue[T any]() *SafeQueue[T] {
	return &SafeQueue[T]{}
}

// Enqueue는 큐 끝에 항목을 추가한다.
func (q *SafeQueue[T]) Enqueue(item T) {
	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()
}

// Dequeue는 큐 앞에서 항목을 꺼낸다. 비어있으면 (zero, false).
func (q *SafeQueue[T]) Dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		var zero T
		return zero, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

// Len은 현재 큐 길이를 반환한다.
func (q *SafeQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Peek은 큐 앞에서 최대 maxCount개 항목을 복사하여 반환한다 (비파괴적).
func (q *SafeQueue[T]) Peek(maxCount int) []T {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := len(q.items)
	if n > maxCount {
		n = maxCount
	}
	result := make([]T, n)
	copy(result, q.items[:n])
	return result
}
