package manager

import (
	"context"
	"fmt"
	"sync"

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
}

// NewManager creates a complete Manager with ManagedFunc.
func NewManager(
	config shared.WatcherConfig,
	name string,
	managedFunc ManagedFunc,
	cleaner Cleaner,
	envVars map[string]string,
) *Manager {
	logger := NewLogger(config.MaxLogEntries)

	state := NewManagerState(shared.ControlIdle)
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
		manageCtx.SetEnvVars(envVars)
	}

	return mgr
}

// Start starts the Reducer loop.
// Only Reducer runs in a goroutine - it calls MgrFuncRunner synchronously.
func (m *Manager) Start(rootCtx context.Context) {
	go m.reducer.Run(rootCtx, m.funcRunner)
}

// GetManagerState returns the current ManagerState. 내부 사용 및 테스트용.
func (m *Manager) GetManagerState() *ManagerState {
	return m.state
}

// --- wm.Subscriber 인터페이스 구현 ---

// GetState는 wm.Subscriber 인터페이스를 충족한다.
func (m *Manager) GetState() (shared.ControlState, shared.RunnerState) {
	return m.state.GetControlState(), m.funcRunner.GetRunnerState()
}

// ReadVarHistory는 wm.Subscriber 인터페이스를 충족한다.
func (m *Manager) ReadVarHistory(varName string) ([]shared.RawWatchValue, error) {
	if m.machineRegistry == nil {
		return nil, fmt.Errorf("no machine registry")
	}
	machine, ok := m.machineRegistry.GetWatchMachine(varName)
	if !ok {
		return nil, fmt.Errorf("wm not found: %s", varName)
	}
	val, alreadyRead := machine.ReadLatestFor(m.name)
	if alreadyRead {
		return nil, nil
	}
	return []shared.RawWatchValue{val}, nil
}

// GetNewSigAppendChan은 wm.Subscriber 인터페이스를 충족한다.
func (m *Manager) GetNewSigAppendChan() <-chan struct{} {
	return m.signals.NewSigAppended
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

// GetSignals returns the SignalChannels.
func (m *Manager) GetSignals() *SignalChannels {
	return m.signals
}

// GetRunner returns the MgrFuncRunner.
func (m *Manager) GetRunner() *MgrfuncRnner {
	return m.funcRunner
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

// Reinitialize clears all Manager state for restart.
// NewSigAppended는 drain하지 않는다 (신호 보존).
func (m *Manager) Reinitialize() {
	m.state.VarState.Clear()
	m.memoCache = sync.Map{}
}
