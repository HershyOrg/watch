package wm

//loop_effect_driven_event.go는 Loop(=Runner)가 Effect 실행 후 반환하는 결과 이벤트임.
//해당 이벤트는 이펙트 후 즉시 재귀적으로 ReduceDriven에 의해 처리되며,
//리듀서의 이벤트 큐에는 큐잉되지 않음.

// LoopEffectDrivenEvent는 Loop가 Effect를 실행한 결과를 Reducer에 알려주는 이벤트임.
type LoopEffectDrivenEvent interface {
	LoopEffectDrivenEvent()
}

// LoopStarted는 StartLoop Effect 성공 결과임.
// Handle 획득 성공 후 Loop 가동이 시작되었음을 나타냄.
type LoopStarted struct{}

func (l *LoopStarted) LoopEffectDrivenEvent() {}

// LoopStartFailed는 StartLoop Effect 실패 결과임.
// Handle 획득에 실패했음을 나타냄.
type LoopStartFailed struct {
	Err error
}

func (l *LoopStartFailed) LoopEffectDrivenEvent() {}

// LoopRecoveryApplied는 TryRecoverLoop Effect 성공 결과임.
// 처방전 적용이 완료되어 재시작 가능함을 나타냄.
type LoopRecoveryApplied struct{}

func (l *LoopRecoveryApplied) LoopEffectDrivenEvent() {}

// LoopRecoveryCrashed는 TryRecoverLoop Effect 실패 결과임.
// 복구 한도를 초과하여 Crash로 전이해야 함을 나타냄.
type LoopRecoveryCrashed struct{}

func (l *LoopRecoveryCrashed) LoopEffectDrivenEvent() {}

// LoopStopCompleted는 StopLoop Effect 완료 결과임.
type LoopStopCompleted struct{}

func (l *LoopStopCompleted) LoopEffectDrivenEvent() {}

// LoopKillCompleted는 KillLoop Effect 완료 결과임.
type LoopKillCompleted struct{}

func (l *LoopKillCompleted) LoopEffectDrivenEvent() {}

// LoopCrashCompleted는 CrashLoop Effect 완료 결과임.
type LoopCrashCompleted struct{}

func (l *LoopCrashCompleted) LoopEffectDrivenEvent() {}
