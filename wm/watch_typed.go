package wm

import (
	"github.com/HershyOrg/watch/shared"
)

// WatchCallFunc는 사용자가 Tick으로 값을 받으며, 필요시 Hook을 적용하기를 선언해주는 함수임.
// WatchCallFunc는 managedCtx의 Manager에 varName에 따른 WatchMachine이 없을 시,
// GetCallHandle을 통해 WatchMachine을 생성함.
// 만약 이미 WatchMachine이 있다면, Manager(=Subscriber)의 VarState에서 값을 받아옴.
// WatchMachine은 백그라운드에서 WatchLoop를 돌리며 VarSig를 Manager(=Subscriber)로 보냄
type WatchCallFunc[T any] func(varName string, setupGetUpdateFunc SetupGetUpdateFunc[T],
	managerCtx shared.ManageContext) (V shared.WatchValue[T])

// WatchFlowFunc는 사용자가 Chan으로 값을 받으며, 필요시 Hook을 적용하기를 선언해주는 함수임.
// WatchCallFunc는 managedCtx의 Manager에 varName에 따른 WatchMachine이 없을 시,
// GetFlowHandle을 통해 WatchMachine을 생성함.
// 만약 이미 WatchMachine이 있다면, Manager의 VarState에서 값을 받아옴.
// WatchMachine은 백그라운드에서 WatchLoop를 돌리며 VarSig를 Manager(=Subscriber)로 보냄
type WatchFlowFunc[T any] func(varName string, getFlowHandle GetFlowChan[T],
	managerCtx shared.ManageContext) (V shared.WatchValue[T])

type SetupGetUpdateFunc[T any] func(callCtx CallContext) (GetUpdateFunc func(runCtx RunContext) UpdateFunc[T], Err error)

// UpdateFunc는 사용자가 전달하는 "이벤트"느낌임.
// 이때, 리듀서 내에서 진행되는 해당 함수는, net호출 등의 부작용을 금지하므로 일부러 ctx를 받지 않게 해둠.
type UpdateFunc[T any] func(prev shared.WatchValue[T]) (next shared.WatchValue[T], skip bool)

type GetFlowChan[T any] func(flowCtx FlowContext) (FlowChan chan UpdateFunc[T], Err error)
