package wm

// PublishersCheckerInterface는 Multi-Manager 확장 시 Publisher 상태를 점검하기 위한 인터페이스임.
// 현재 단일 Manager 버전에서는 런타임 기능 범위 밖이며, event loop에서 호출하지 않음.
// 다만 예시를 들자면, 만약 해당 WatchMachine을 Export한 Publisher(=Manager)가 Crashed시,
// WatchLoop도 Crashed로 전이해야 하는 것임.
// WatchLoop는 Loop자체의 신호만으론 Publisher의 상태를 파악하지 못하므로, 별도 체크 루틴이 필요함.
type PublishersCheckerInterface interface {
	CheckIfPublishersBad() LoopEvent
}
