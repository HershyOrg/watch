package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// WatchHandle is an interface for different types of watch mechanisms.
type WatchHandle interface {
	GetVarName() string
	GetCancelFunc() context.CancelFunc
}

// TickHandle represents a tick-based watch variable.
type TickHandle struct {
	VarName            string
	GetComputationFunc wm.DELETED_GetComputationFunc
	Tick               time.Duration
	CancelFunc         context.CancelFunc
}

func (h *TickHandle) GetVarName() string                { return h.VarName }
func (h *TickHandle) GetCancelFunc() context.CancelFunc { return h.CancelFunc }

// FlowHandle represents a channel-based watch variable.
type FlowHandle struct {
	VarName        string
	GetChannelFunc func(ctx context.Context) (<-chan shared.RawFlowValue, error)
	CancelFunc     context.CancelFunc
}

func (h *FlowHandle) GetVarName() string                { return h.VarName }
func (h *FlowHandle) GetCancelFunc() context.CancelFunc { return h.CancelFunc }

// Manager encapsulates all Manager components.
// It orchestrates the Reducer-Target pattern for reactive execution.
type Manager struct {
	config *shared.WatcherConfig

	logger *Logger

	state   *ManagerState
	signals *SignalChannels

	reducer *Reducer
	target  *Target

	memoCache     sync.Map // map[string]any
	watchRegistry sync.Map // map[string]WatchHandle
}

// NewManager creates a complete Manager with ManagedFunc.
func NewManager(
	config shared.WatcherConfig,
	managedFunc ManagedFunc,
	cleaner Cleaner,
	envVars map[string]string,
) *Manager {
	logger := NewLogger(config.MaxLogEntries)

	state := NewManagerState(shared.ControlIdle)
	signals := NewSignalChannels(config.SignalChanCapacity)

	reducer := NewReducer(state, signals, logger)

	target := NewTarget(
		managedFunc,
		cleaner,
		logger,
		config,
	)

	mgr := &Manager{
		config:        &config,
		logger:        logger,
		state:         state,
		signals:       signals,
		reducer:       reducer,
		target:        target,
		memoCache:     sync.Map{},
		watchRegistry: sync.Map{},
	}

	// Set Manager reference in Target for reinitialization
	target.SetManager(mgr)

	// Get ManageContext from Target and set Manager reference
	if manageCtx, ok := target.GetManageContext().(*ManageContext); ok {
		manageCtx.SetManager(mgr)
		manageCtx.SetEnvVars(envVars)
	}

	return mgr
}

// Start starts the Reducer loop.
// Only Reducer runs in a goroutine - it calls Target synchronously.
func (m *Manager) Start(rootCtx context.Context) {
	go m.reducer.Run(rootCtx, m.target)
}

// GetState returns the current ManagerState.
func (m *Manager) GetState() *ManagerState {
	return m.state
}

// GetSignals returns the SignalChannels.
func (m *Manager) GetSignals() *SignalChannels {
	return m.signals
}

// GetTarget returns the Target.
func (m *Manager) GetTarget() *Target {
	return m.target
}

// GetLogger returns the Manager's logger.
func (m *Manager) GetLogger() *Logger {
	return m.logger
}

// GetMemoCache returns a pointer to the memo cache.
func (m *Manager) GetMemoCache() *sync.Map {
	return &m.memoCache
}

// GetWatchRegistry returns a pointer to the watch registry.
func (m *Manager) GetWatchRegistry() *sync.Map {
	return &m.watchRegistry
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

// RegisterWatch registers a Watch variable with limit enforcement.
func (m *Manager) RegisterWatch(varName string, handle WatchHandle) error {
	if _, exists := m.watchRegistry.Load(varName); exists {
		return fmt.Errorf("watch varName already exists: %s", varName)
	}

	count := 0
	m.watchRegistry.Range(func(_, _ any) bool {
		count++
		return true
	})

	if count >= m.config.MaxWatches {
		return fmt.Errorf("watch registry limit reached: %d/%d (cannot register '%s')",
			count, m.config.MaxWatches, varName)
	}

	m.watchRegistry.Store(varName, handle)
	return nil
}

// Reinitialize clears all Manager state for restart.
func (m *Manager) Reinitialize() {
	m.state.VarState.Clear()

	m.watchRegistry.Range(func(key, value any) bool {
		handle := value.(WatchHandle)
		if cancel := handle.GetCancelFunc(); cancel != nil {
			cancel()
		}
		return true
	})
	m.watchRegistry = sync.Map{}

	m.memoCache = sync.Map{}

	for {
		select {
		case <-m.signals.VarSigChan:
		default:
			return
		}
	}
}
