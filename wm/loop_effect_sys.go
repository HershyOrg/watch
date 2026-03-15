package wm

type LoopEffectHandlerInferface interface{}
type LoopEffect interface {
	LoopEffect()
}

// StartLoop는 WatchLoop를 "초기화 후 시작"하는 Effect임
// 기존의 RootContext를 제거 후, 새 Context와 새 Handle과 함께 Loop자체를 초기화함.
type StartLoop struct{}

func (s *StartLoop) LoopEffect() {}

// TryRecoverLoop는 WatchLoop에 대한 "처방전"을 적용하는 Effect임
type TryRecoverLoop struct {
	//* 여긴 리커버리 폴리시 맞게 수정하기
	//*함수의 연속열을 넣어줘도 좋을듯. sleep하고, 재시작하고 등등.
	//*여기선 "Loop에 조작 가하기"의 의미로 이 임시 함수 시그니처 썼음.
	ApplyPrescription []func(loop WatchLoopInterface)
}

func (t *TryRecoverLoop) LoopEffect() {}

// StopLoop는 Loop를 멈추는 Effect임. 멈출 시, 2개의 ctx를 전부 종료함
type StopLoop struct{}

func (s *StopLoop) LoopEffect() {}

// CrashLoop는 Loop를 멈춘 후, Crash로 전이하게 하는 Effect임
type CrashLoop struct{}

func (c *CrashLoop) LoopEffect() {}
