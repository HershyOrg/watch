package wm

// LoopReducerInterface는 Loop의 Reducer가 해야 할 일에 대한 디자인임.
type LoopReducerInterface interface {
	//Reduce는 외부 주입 이벤트(LoopEvent)를 받아 상태 전이와 이펙트를 결정함.
	Reduce(currentState LoopState, event LoopEvent) (nextState LoopState, effects []LoopEffect)
	//ReduceDriven은 Loop(=Runner)가 Effect 실행 후 반환한 결과 이벤트를 받아
	//재귀적으로 상태 전이와 추가 이펙트를 결정함.
	ReduceDriven(currentState LoopState, driven LoopEffectDrivenEvent) (nextState LoopState, effects []LoopEffect)
}

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

// LoopStopped는 Loop가 멈췄음을 나타냄.
type LoopStopped struct{}

func (ls *LoopStopped) LoopState() { return }

// LoopCrashed는 Loop가 복구 불가로 멈췄음을 나타냄.
type LoopCrashed struct{}

func (lc *LoopCrashed) LoopState() { return }
