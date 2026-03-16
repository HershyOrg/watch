package wm

//loop_effect_driven_event.go는 loop가 effect핸들링 후 발생시키는 확인신호 event임.
//해당 이벤트는 이펙트 후 즉시 재귀적으로 처리되며, 리듀서의 이벤트 큐에는 큐잉되지 않음.


