package wm

// LoopReducerInterface는 Loop의 Reducer가 해야 할 일에 대한 디자인임.
type LoopReducerInterface interface {
	//LoopReducerInterface의 Reduce함수는
	//"WatchLoop"의 State, Effect를 다룸
	Reduce(currentState LoopState, event LoopEvent) (nextState LoopState, effects []LoopEffect)
}

// LoopState는 WatchLoop의 상태임
type LoopState interface {
	LoopState()
}

// LoopRunning은 Loop가 정상 작동중임을 나타냄
type LoopRunning struct{}

func (lr *LoopRunning) LoopState() { return }

// LoopTryingRecovery는 Loop가 리커버리 시도중임을 나타냄.
type LoopTryingRecovery struct{}

func (lt *LoopTryingRecovery) LoopState() { return }

// LoopStopped는 Loop가 멈췄음을 나타냄.
type LoopStopped struct{}

func (ls *LoopStopped) LoopState() { return }

// LoopCrashed는 Loop가 복구 불가로 멈췄음을 나타냄.
type LoopCrashed struct{}

func (lc *LoopCrashed) LoopState() { return }
