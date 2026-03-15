package wm

import "context"


// CallContext는 RunContext와 동일함.
// CallContext타입을 요구하는 곳에는, RunContext를 집어넣음.
// 다만 의미론의 이유에서 RunContext의 별명으로써 CallContext를 정의함.
type CallContext interface {
	RunContext
}

// RunContext는 WatchMachine이 "한 회 실행"할 때마다 부여하는 Context임.
// 타임아웃이나 블로킹 에러를 막기 위해 사용됨.
// 주로 1분 타임아웃을 걺.
type RunContext interface {
	context.Context
	RunContext()
}

// FlowContext는 RootContext와 동일함.
// FlowContext타입을 요구하는 곳에는, RootContext를 집어넣음.
// 다만 의미론의 이유에서 RootContext의 별명으로써 FlowContext를 정의함.
type FlowContext interface {
	RootContext
}

// RootContext는 WatchMachine의 생명주기와 직결된 RootContext임.
// RootContext종료 시 WatchMachine의 모든 goroutine은 종료됨.
// 별도의 타임아웃이 없으며, WatchMachine이 Stop/Crashed시 RootContext도 cancel됨.
type RootContext interface {
	context.Context
	RootContext()
}
