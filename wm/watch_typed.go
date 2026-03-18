package wm

import (
	"time"

	"github.com/HershyOrg/watch/shared"
)

// WatchCallFunc는 사용자가 Tick으로 값을 받으며, 필요시 Hook을 적용하기를 선언해주는 함수임.
// WatchCallFunc는 managedCtx의 Manager에 varName에 따른 WatchMachine이 없을 시,
// GetCallHandle을 통해 WatchMachine을 생성함.
// 만약 이미 WatchMachine이 있다면, Manager(=Subscriber)의 VarState에서 값을 받아옴.
// WatchMachine은 백그라운드에서 WatchLoop를 돌리며 VarSig를 Manager(=Subscriber)로 보냄
type WatchCallFunc[T any] func(varName string, getCallHandle GetCallHandleFunc[T],
	managerCtx shared.ManageContext) (V shared.WatchValue[T])

// WatchFlowFunc는 사용자가 Chan으로 값을 받으며, 필요시 Hook을 적용하기를 선언해주는 함수임.
// WatchCallFunc는 managedCtx의 Manager에 varName에 따른 WatchMachine이 없을 시,
// GetFlowHandle을 통해 WatchMachine을 생성함.
// 만약 이미 WatchMachine이 있다면, Manager의 VarState에서 값을 받아옴.
// WatchMachine은 백그라운드에서 WatchLoop를 돌리며 VarSig를 Manager(=Subscriber)로 보냄
type WatchFlowFunc[T any] func(varName string, getFlowHandle GetFlowHandleFunc[T],
	managerCtx shared.ManageContext) (V shared.WatchValue[T])

type GetCallHandleFunc[T any] func(callCtx CallContext) (CallHandle[T], error)

type CallHandle[T any] struct {
	//Tick마다 GetUpdateFunc를 가져오는 것 = Chan으로 UpdateFunc를 가져오는 것과 구조적 동일함
	Tick time.Duration
	//*GetUpdateFunc는 error를 반환하지 않음.
	//*에러 발생 시 그 에러를 그냥 UpdateFunc의 return값이 에러가 되게 UpdateFunc를 만듦.
	GetUpdateFunc func(runCtx RunContext) UpdateFunc[T]
	//* varName은 WatchCall이 varName받은 후 삽입.
	varName string
}

// UpdateFunc는 사용자가 전달하는 "이벤트"느낌임.
// 이때, 리듀서 내에서 진행되는 해당 함수는, net호출 등의 부작용을 금지하므로 일부러 ctx를 받지 않게 해둠.
type UpdateFunc[T any] func(prev shared.WatchValue[T]) (next shared.WatchValue[T], skip bool)

type GetFlowHandleFunc[T any] func(flowCtx FlowContext) (FlowHandle[T], error)
type FlowHandle[T any] struct {
	FlowChan chan UpdateFunc[T]
	//* varName은 WatchFlow가 varName받은 후 삽입
	varName string
}
