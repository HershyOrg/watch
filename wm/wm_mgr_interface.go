package wm

import "github.com/HershyOrg/watch/shared"

// WatchMachine이 Manager를 "구독자"로 바라봤을 떄의 interface임
// Manager는 WatchMachine을 "구독"함
// Subscriber(Manager)는 WatchMachine에게 "수집한 valueLog"를 얻어옮.
type Subscriber interface {
	//GetVarHistory는 Subscriber가 watchMachine에서 VarHistory꺼내오는 것임.
	ReadVarHistory(varName string) ([]shared.RawWatchValue, error)
	GetState() (controlState shared.ControlState, runnerState shared.RunnerState)
	GetNewSigAppendChan() <-chan struct{}
}

// Publisher는 WatchMachine이 Manager를 "발행자"로 바라봤을 때의 interface임.
// 이는 추후 Multi-Manager기능 시 유효해짐.
// 지금 당장은 세세하게 다루지 않음.
type Publisher interface {
	PushUpdateFunc(varName string, updateFunc RawUpdateFunc)
}
