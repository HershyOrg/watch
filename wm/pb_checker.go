package wm

// PublishersCheckerInterface는 PublishChecker가 해야 할 일을 정의한 인터페이스임.
// PublishChecker는 Publisher의 상태를 체크해서, WatchLoop의 종료 여부를 판단함.
// 이는 Multi-Manager가 구현되었을 시 필요한 기능이므로, 지금 단계에선 고려하지 않음.
// 다만 예시를 들자면, 만약 해당 WatchMachine을 Export한 Publisher(=Manager)가 Crashed시,
// WatchLoop도 Crashed로 전이해야 하는 것임.
// WatchLoop는 Loop자체의 신호만으론 Publisher의 상태를 파악하지 못하므로, 별도 체크 루틴이 필요함.
type PublishersCheckerInterface interface {
	CheckIfPublishersBad() LoopEvent
}
