package wm

// LoopReducer는 LoopReducerInterface의 구체 구현임.
// 순수 함수형: 외부에서 state를 주입받아 전이를 결정함.
// 리커버리는 현 단계에서 미구현: 에러 발생 시 바로 Crash 처리.
//
// Terminal state: LoopKilled, LoopCrashed (재시작 불가)
// Non-terminal stop: LoopStopped (재시작 가능)
type LoopReducer struct{}

func (r *LoopReducer) Reduce(current LoopState, event LoopEvent) (LoopState, []LoopEffect) {
	// terminal state에선 모든 이벤트 무시
	switch current.(type) {
	case *LoopKilled, *LoopCrashed:
		return current, nil
	}

	switch current.(type) {
	case *LoopIdle:
		return r.reduceIdle(event)
	case *LoopStopped:
		return r.reduceStopped(event)
	case *LoopRunning:
		return r.reduceRunning(event)
	case *LoopStarting:
		return current, nil
	}

	return current, nil
}

func (r *LoopReducer) ReduceDriven(current LoopState, driven LoopEffectDrivenEvent) (LoopState, []LoopEffect) {
	// terminal state에선 모든 DrivenEvent 무시
	switch current.(type) {
	case *LoopKilled, *LoopCrashed:
		return current, nil
	}

	switch d := driven.(type) {
	case *LoopStarted:
		return &LoopRunning{}, nil

	case *LoopGotErrFromGetHandle:
		_ = d
		return current, []LoopEffect{&CrashLoop{}}

	case *LoopStartFailed:
		return current, []LoopEffect{&CrashLoop{}}

	case *LoopStopCompleted:
		return &LoopStopped{}, nil

	case *LoopKillCompleted:
		return &LoopKilled{}, nil

	case *LoopCrashCompleted:
		return &LoopCrashed{}, nil
	}

	return current, nil
}

func (r *LoopReducer) reduceIdle(event LoopEvent) (LoopState, []LoopEffect) {
	switch event.(type) {
	case *StartRequested:
		return &LoopStarting{}, []LoopEffect{&StartLoop{}}
	case *StopRequested:
		return &LoopStopped{}, nil
	case *KillRequested:
		return &LoopKilled{}, nil
	case *CrashRequested:
		return &LoopCrashed{}, nil
	}
	return &LoopIdle{}, nil
}

// reduceStopped — LoopStopped는 재시작 가능한 상태
func (r *LoopReducer) reduceStopped(event LoopEvent) (LoopState, []LoopEffect) {
	switch event.(type) {
	case *StartRequested:
		return &LoopStarting{}, []LoopEffect{&StartLoop{}}
	case *KillRequested:
		return &LoopKilled{}, nil
	case *CrashRequested:
		return &LoopCrashed{}, nil
	}
	return &LoopStopped{}, nil
}

func (r *LoopReducer) reduceRunning(event LoopEvent) (LoopState, []LoopEffect) {
	switch event.(type) {
	case *StopRequested:
		return &LoopRunning{}, []LoopEffect{&StopLoop{}}
	case *KillRequested:
		return &LoopRunning{}, []LoopEffect{&KillLoop{}}
	case *CrashRequested:
		return &LoopRunning{}, []LoopEffect{&CrashLoop{}}
	}
	return &LoopRunning{}, nil
}
