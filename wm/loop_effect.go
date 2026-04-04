package wm

type LoopEffect interface {
	LoopEffect()
}

// StartLoop는 WatchLoop를 "초기화 후 시작"하는 Effect임
// 기존의 RootContext를 제거 후, 새 Context와 새 Handle과 함께 Loop자체를 초기화함.
type StartLoop struct{}

func (s *StartLoop) LoopEffect() { return }

// TryRecoverLoop는 WatchLoop에 대한 "처방전"을 적용하는 Effect임
type TryRecoverLoop struct {
}

func (t *TryRecoverLoop) LoopEffect() { return }

// StopLoop는 Loop를 멈추는 Effect임. 멈출 시, 2개의 ctx를 전부 종료함
type StopLoop struct{}

func (s *StopLoop) LoopEffect() { return }

type KillLoop struct{}

func (k *KillLoop) LoopEffect() { return }

// CrashLoop는 Loop를 멈춘 후, Crash로 전이하게 하는 Effect임
type CrashLoop struct{}

func (c *CrashLoop) LoopEffect() { return }
