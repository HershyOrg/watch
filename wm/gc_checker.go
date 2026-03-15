package wm

// GcCheckerInterface는 GcChecker가 해야 할 일을 정의한 인터페이스임.
// GcChecker는 WatchMachine의 Subscriber들 상태를 체크해서, WatchLoop의 "종료"여부를 판단함.
// 예컨데, WatchMachine의 Subscribers가 모두 Crashed라면,
// WatchLoop은 더이상 돌아갈 이유가 없으며,자신 역시 Stop함.
type GcCheckerInterface interface {
	CheckIfSubscribersBad() LoopEvent
}
