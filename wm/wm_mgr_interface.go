package wm

import "github.com/HershyOrg/watch/shared"

// WatchMachine이 Manager를 "구독자"로 바라봤을 때의 interface임.
// Manager는 WatchMachine을 "구독"함.
// Subscriber(Manager)는 WatchMachine에게서 수집한 valueLog를 얻어옴.
type Subscriber interface {
	// GetState는 Subscriber의 현재 상태를 반환함.
	GetState() (controlState shared.ControlState, runnerState shared.RunnerState)

	// GetName은 구독자 식별용 이름을 반환함.
	GetName() string
}

// Publisher는 WatchMachine이 Manager를 "발행자"로 바라봤을 때의 interface임.
// 이는 추후 Multi-Manager기능 시 유효해짐.
// 지금 당장은 세세하게 다루지 않음.
type Publisher interface {
	PushUpdateFunc(varName string, updateFunc RawUpdateFunc)
}
