package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// Manager encapsulates all Manager components.
// It orchestrates the Reducer-MgrFuncRunner pattern for reactive execution.
// Manager는 wm.Subscriber 인터페이스를 직접 구현한다.
type Manager struct {
	config *shared.WatcherConfig
	name   string // managerName (Subscriber 식별용)

	logger *Logger

	state   *ManagerState
	signals *SignalChannels

	reducer    *Reducer
	funcRunner *MgrfuncRnner

	memoCache       sync.Map           // map[string]any
	machineRegistry wm.MachineRegistry // Watcher가 설정

	cancelEventLoop context.CancelFunc // 이벤트 루프 종료용
}

// NewManager creates a complete Manager with ManagedFunc.
func NewManager(
	config shared.WatcherConfig,
	name string,
	managedFunc ManagedFunc,
	cleaner Cleaner,
	envVars map[string]any,
) *Manager {
	logger := NewLogger(config.MaxLogEntries)

	state := NewManagerState(&shared.ControlIdle{})
	signals := NewSignalChannels(config.SignalChanCapacity)

	runner := NewRunner(
		managedFunc,
		cleaner,
		logger,
		config,
	)

	mgr := &Manager{
		config:     &config,
		name:       name,
		logger:     logger,
		state:      state,
		signals:    signals,
		funcRunner: runner,
		memoCache:  sync.Map{},
	}

	// Reducer는 Manager 참조 필요 (MachineRegistry 접근용)
	mgr.reducer = NewReducer(state, signals, logger, mgr)

	// Set Manager reference in MgrFuncRunner for reinitialization
	runner.SetManager(mgr)

	// Get ManageContext from MgrFuncRunner and set Manager reference
	if manageCtx, ok := runner.GetManageContext().(*ManageContext); ok {
		manageCtx.SetManager(mgr)
		manageCtx.SetFrozenValues(envVars)
	}

	// 자체 ctx로 리듀서 이벤트 루프 즉시 시작 (WatchMachine 패턴)
	ctx, cancel := context.WithCancel(context.Background())
	mgr.cancelEventLoop = cancel
	go mgr.reducer.Run(ctx, mgr.funcRunner)

	return mgr
}

// Close는 이벤트 루프를 종료한다 (WatchMachine.Close()와 동일).
func (m *Manager) Close() {
	if m.cancelEventLoop != nil {
		m.cancelEventLoop()
	}
}

// --- 이벤트 기반 제어 메서드 (WatchMachine 패턴) ---

// RequestStop은 Manager에 정지 이벤트를 전송한다.
func (m *Manager) RequestStop(reason string) {
	m.signals.SendControlEvent(&ControlEvent{
		ReceivedTime: time.Now(),
		Kind:         StopRequested,
		Reason:       reason,
	})
}

// RequestKill은 Manager에 강제 종료 이벤트를 전송한다.
func (m *Manager) RequestKill(reason string) {
	m.signals.SendControlEvent(&ControlEvent{
		ReceivedTime: time.Now(),
		Kind:         KillRequested,
		Reason:       reason,
	})
}

// RequestRun은 Manager에 실행 이벤트를 전송한다 (터미널 상태에서 재시작용).
func (m *Manager) RequestRun(reason string, needInit bool) {
	m.signals.SendControlEvent(&ControlEvent{
		ReceivedTime: time.Now(),
		Kind:         RunRequested,
		NeedInit:     needInit,
		Reason:       reason,
	})
}

// SendUserEvent는 유저 메시지 이벤트를 전송한다.
func (m *Manager) SendUserEvent(msg *shared.Message) {
	m.signals.SendUserEvent(&UserMessageReceived{
		ReceivedTime: time.Now(),
		UserMessage:  msg,
	})
}

// --- 상태 조회 메서드 (내부 노출 없이) ---

// GetControlState는 현재 ControlState를 반환한다.
func (m *Manager) GetControlState() shared.ControlState {
	return m.state.GetControlState()
}

// IsCleanupCompleted는 Runner의 cleanup 완료 여부를 반환한다.
func (m *Manager) IsCleanupCompleted() bool {
	return m.funcRunner.IsCleanupCompleted()
}

// GetNewSigAppendedChan은 NewSigAppended 채널을 반환한다 (WatchMachine 구독 알림용).
func (m *Manager) GetNewSigAppendedChan() chan struct{} {
	return m.signals.NewSigAppended
}

// GetSignalQueueLengths는 각 시그널 큐의 현재 길이를 반환한다 (API 모니터링용).
func (m *Manager) GetSignalQueueLengths() (userCount, controlCount int) {
	return m.signals.UserQueueLen(), m.signals.ControlQueueLen()
}

// PeekSignals는 큐에 있는 시그널들을 non-destructive peek한다 (API 모니터링용).
func (m *Manager) PeekSignals(maxCount int) []SignalPeekEntry {
	return m.signals.PeekSignals(maxCount)
}

// GetManagerState returns the current ManagerState. 테스트 전용.
func (m *Manager) GetManagerState() *ManagerState {
	return m.state
}

// --- wm.Subscriber 인터페이스 구현 ---

// GetState는 wm.Subscriber 인터페이스를 충족한다.
func (m *Manager) GetState() (shared.ControlState, shared.RunnerState) {
	return m.state.GetControlState(), m.funcRunner.GetRunnerState()
}

// GetName은 wm.Subscriber 인터페이스를 충족한다.
func (m *Manager) GetName() string {
	return m.name
}

// SetMachineRegistry는 Watcher가 Manager에 MachineRegistry를 설정할 때 사용.
func (m *Manager) SetMachineRegistry(registry wm.MachineRegistry) {
	m.machineRegistry = registry
}

// GetMachineRegistry는 MachineRegistry를 반환한다.
func (m *Manager) GetMachineRegistry() wm.MachineRegistry {
	return m.machineRegistry
}

// SetCleaner는 Manager의 MgrFuncRunner에 Cleaner를 설정한다.
func (m *Manager) SetCleaner(cleaner Cleaner) {
	m.funcRunner.SetCleaner(cleaner)
}

// GetLogger returns the Manager's logger.
func (m *Manager) GetLogger() *Logger {
	return m.logger
}

// GetMemoCache returns a pointer to the memo cache.
func (m *Manager) GetMemoCache() *sync.Map {
	return &m.memoCache
}

// GetConfig returns the WatcherConfig.
func (m *Manager) GetConfig() *shared.WatcherConfig {
	return m.config
}

// SetMemo stores a value in the memo cache with size limit enforcement.
func (m *Manager) SetMemo(key string, value any) error {
	if _, exists := m.memoCache.Load(key); !exists {
		count := 0
		m.memoCache.Range(func(_, _ any) bool {
			count++
			return true
		})

		if count >= m.config.MaxMemoEntries {
			return fmt.Errorf("memo cache limit reached: %d/%d (cannot cache '%s')",
				count, m.config.MaxMemoEntries, key)
		}
	}

	m.memoCache.Store(key, value)
	return nil
}

// GetMemo retrieves a value from the memo cache.
func (m *Manager) GetMemo(key string) (any, bool) {
	return m.memoCache.Load(key)
}
