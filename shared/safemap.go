package shared

import "sync"

// SafeMap은 타입 안전한 병렬 안전 map이다.
type SafeMap[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// NewSafeMap은 새 SafeMap을 생성한다.
func NewSafeMap[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		data: make(map[K]V),
	}
}

// Store는 키-값을 저장한다.
func (m *SafeMap[K, V]) Store(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Load는 키에 해당하는 값을 반환한다.
func (m *SafeMap[K, V]) Load(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[key]
	return val, ok
}

// Delete는 키를 삭제한다.
func (m *SafeMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// Range는 모든 키-값에 대해 fn을 호출한다. fn이 false를 반환하면 중단.
func (m *SafeMap[K, V]) Range(fn func(K, V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.data {
		if !fn(k, v) {
			break
		}
	}
}

// Values는 모든 값의 슬라이스를 반환한다.
func (m *SafeMap[K, V]) Values() []V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]V, 0, len(m.data))
	for _, v := range m.data {
		result = append(result, v)
	}
	return result
}

// Len은 저장된 항목 수를 반환한다.
func (m *SafeMap[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}
