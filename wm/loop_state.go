package wm


// LoopState는 WatchLoop의 상태임
type LoopState interface {
	LoopState()
}

// LoopIdle은 Loop가 생성 직후, 아직 Start되지 않은 상태임.
type LoopIdle struct{}

func (li *LoopIdle) LoopState() { return }

// LoopStarting은 Loop가 Handle 획득을 시도하고 있는 상태임.
type LoopStarting struct{}

func (ls *LoopStarting) LoopState() { return }

// LoopRunning은 Loop가 정상 작동중임을 나타냄
type LoopRunning struct{}

func (lr *LoopRunning) LoopState() { return }

// LoopTryingRecovery는 Loop가 리커버리 시도중임을 나타냄.
type LoopTryingRecovery struct{}

func (lt *LoopTryingRecovery) LoopState() { return }

// LoopStopped는 Loop가 정상 정지됨을 나타냄.
// 재시작 가능한 상태임 (StartRequested로 다시 시작 가능).
type LoopStopped struct{}

func (ls *LoopStopped) LoopState() { return }

// LoopKilled는 Loop가 강제 종료됨을 나타냄.
// 완전 종료(terminal) 상태. 재시작 불가.
type LoopKilled struct{}

func (lk *LoopKilled) LoopState() { return }

// LoopCrashed는 Loop가 복구 불가로 멈췄음을 나타냄.
// 완전 종료(terminal) 상태. 재시작 불가.
type LoopCrashed struct{}

func (lc *LoopCrashed) LoopState() { return }
