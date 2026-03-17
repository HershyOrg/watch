package wm

// LoopReducer는 순수 함수형 Reducer.
// 외부에서 state를 주입받아 전이를 결정함.
//
// Terminal state: LoopKilled, LoopCrashed (재시작 불가)
// Non-terminal stop: LoopStopped (재시작 가능)
type LoopReducer struct {
	loopHistory    *LoopHistory
	recoveryPolicy LoopRecoveryPolicy
}

func (r *LoopReducer) Reduce(current LoopState, event LoopEvent) (LoopState, []LoopEffect) {
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
	case *LoopTryingRecovery:
		return current, nil
	}

	return current, nil
}

func (r *LoopReducer) ReduceDriven(current LoopState, driven LoopEffectDrivenEvent) (LoopState, []LoopEffect) {
	switch current.(type) {
	case *LoopKilled, *LoopCrashed:
		return current, nil
	}

	switch driven.(type) {
	case *LoopStarted:
		return &LoopRunning{}, nil

	case *LoopGotErrFromGetHandle:
		// GetHandle 실패 → 연속 에러 체크 후 리커버리 or Crash
		consecutiveErrs := r.loopHistory.ConsecutiveErrors()
		if consecutiveErrs >= r.recoveryPolicy.MaxGetHandleRetries {
			return current, []LoopEffect{&CrashLoop{}}
		}
		return &LoopTryingRecovery{}, []LoopEffect{&TryRecoverLoop{}}

	case *LoopStartFailed:
		return current, []LoopEffect{&CrashLoop{}}

	case *LoopRecoveryApplied:
		// 리커버리 완료 → 다시 StartLoop
		return &LoopStarting{}, []LoopEffect{&StartLoop{}}

	case *LoopRecoveryCrashed:
		return current, []LoopEffect{&CrashLoop{}}

	case *LoopStopCompleted:
		if _, ok := current.(*LoopTryingRecovery); ok {
			// RecoveryRequested에 의한 Stop완료 → TryRecoverLoop 발행
			return &LoopTryingRecovery{}, []LoopEffect{&TryRecoverLoop{}}
		}
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
	case *RecoveryRequested:
		// Tier 2: 현재 Loop Stop → TryingRecovery로 전이
		consecutiveErrs := r.loopHistory.ConsecutiveErrors()
		if consecutiveErrs >= r.recoveryPolicy.MaxConsecutiveFailures {
			return &LoopRunning{}, []LoopEffect{&CrashLoop{}}
		}
		return &LoopTryingRecovery{}, []LoopEffect{&StopLoop{}}
	}
	return &LoopRunning{}, nil
}
