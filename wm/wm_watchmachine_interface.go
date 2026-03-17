package wm

// MachineRegistry는 WatchMachine과 Manager가 바라본 Watcher의 인터페이스디ㅏ.
// Wm, Mgr은 Watcher를 통해 Wm을 등록-조회한다.
type MachineRegistry interface {
	RegisterWatchMachine(managerName string, watchMachine *WatchMachine) error
}
