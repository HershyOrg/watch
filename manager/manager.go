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
	GetComputationFunc wm.DELETED_GetComputationFunc // Returns a function to compute next state and skipSignal flag
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
// It orchestrates the Reducer-Effect pattern for reactive execution.
type Manager struct {
	config *shared.WatcherConfig

	logger *Logger

	state   *ManagerState
	signals *SignalChannels

	reducer   *Reducer
	commander *EffectCommander
	handler   *EffectHandler

	memoCache     sync.Map // map[string]any
	watchRegistry sync.Map // map[string]WatchHandle
}

// NewManager creates a complete Manager with ManagedFunc.
// The Manager is fully initialized and ready to start.
func NewManager(
	config shared.WatcherConfig,
	managedFunc ManagedFunc,
	cleaner Cleaner,
	envVars map[string]string,
) *Manager {
	// Initialize logger with config limit
	logger := NewLogger(config.MaxLogEntries)

	// Initialize Manager components (start in Ready)
	state := NewManagerState(shared.StateReady)
	signals := NewSignalChannels(config.SignalChanCapacity)

	// Create reducer
	reducer := NewReducer(state, signals, logger)

	// Create commander
	commander := NewEffectCommander()

	// Create handler with ManagedFunc
	handler := NewEffectHandler(
		managedFunc, // Passed at creation
		cleaner,
		state,
		signals,
		logger,
		config,
	)

	mgr := &Manager{
		config:        &config,
		logger:        logger,
		state:         state,
		signals:       signals,
		reducer:       reducer,
		commander:     commander,
		handler:       handler,
		memoCache:     sync.Map{},
		watchRegistry: sync.Map{},
	}

	// Set Manager reference in EffectHandler for reinitialization
	handler.SetManager(mgr)

	// Get ManageContext from EffectHandler and set Manager reference
	if manageCtx, ok := handler.GetManageContext().(*ManageContext); ok {
		manageCtx.SetManager(mgr)
		manageCtx.SetEnvVars(envVars)
	}

	return mgr
}

// Start starts the Reducer loop (synchronous architecture).
// Only Reducer runs in a goroutine now - it calls Commander and Handler synchronously.
func (wm *Manager) Start(rootCtx context.Context) {
	// Pass commander and handler to reducer so it can call them synchronously
	go wm.reducer.RunWithEffects(rootCtx, wm.commander, wm.handler)
}

// GetState returns the current ManagerState.
func (wm *Manager) GetState() *ManagerState {
	return wm.state
}

// GetSignals returns the SignalChannels.
func (wm *Manager) GetSignals() *SignalChannels {
	return wm.signals
}

// GetEffectHandler returns the EffectHandler.
func (wm *Manager) GetEffectHandler() *EffectHandler {
	return wm.handler
}

// GetLogger returns the Manager's logger.
func (wm *Manager) GetLogger() *Logger {
	return wm.logger
}

// GetMemoCache returns a pointer to the memo cache.
func (wm *Manager) GetMemoCache() *sync.Map {
	return &wm.memoCache
}

// GetWatchRegistry returns a pointer to the watch registry.
func (wm *Manager) GetWatchRegistry() *sync.Map {
	return &wm.watchRegistry
}

// GetConfig returns the WatcherConfig.
func (wm *Manager) GetConfig() *shared.WatcherConfig {
	return wm.config
}

// SetMemo stores a value in the memo cache with size limit enforcement.
// Returns error if cache limit is reached.
func (wm *Manager) SetMemo(key string, value any) error {
	// Check if updating existing entry (allowed)
	if _, exists := wm.memoCache.Load(key); !exists {
		// New entry - check size limit
		count := 0
		wm.memoCache.Range(func(_, _ any) bool {
			count++
			return true
		})

		if count >= wm.config.MaxMemoEntries {
			return fmt.Errorf("memo cache limit reached: %d/%d (cannot cache '%s')",
				count, wm.config.MaxMemoEntries, key)
		}
	}

	wm.memoCache.Store(key, value)
	return nil
}

// GetMemo retrieves a value from the memo cache.
func (wm *Manager) GetMemo(key string) (any, bool) {
	return wm.memoCache.Load(key)
}

// RegisterWatch registers a Watch variable with limit enforcement.
// Watch variables are immutable once registered.
// This is called by WatchCall/WatchFlow functions.
// Returns error if watch limit is reached or if watch already exists.
func (wm *Manager) RegisterWatch(varName string, handle WatchHandle) error {
	// Check if watch already exists
	if _, exists := wm.watchRegistry.Load(varName); exists {
		return fmt.Errorf("watch varName already exists: %s", varName)
	}

	// Check size limit
	count := 0
	wm.watchRegistry.Range(func(_, _ any) bool {
		count++
		return true
	})

	if count >= wm.config.MaxWatches {
		return fmt.Errorf("watch registry limit reached: %d/%d (cannot register '%s')",
			count, wm.config.MaxWatches, varName)
	}

	wm.watchRegistry.Store(varName, handle)
	return nil
}

// Reinitialize clears all Manager state for restart.
// This is called when Manager transitions from terminal states (Stopped/Killed/Crashed) to Running.
// Clears: VarState, WatchRegistry, MemoCache, VarSig channel.
// Preserves: Logger, Config, UserState, ManagerInnerState, ManagedFunc.
func (wm *Manager) Reinitialize() {
	// 1. Clear VarState
	wm.state.VarState.Clear()

	// 2. Cancel and clear all Watch handles
	wm.watchRegistry.Range(func(key, value any) bool {
		handle := value.(WatchHandle)
		if cancel := handle.GetCancelFunc(); cancel != nil {
			cancel()
		}
		return true
	})
	// Create new empty registry
	wm.watchRegistry = sync.Map{}

	// 3. Clear MemoCache
	wm.memoCache = sync.Map{}

	// 4. Drain VarSig channel to prevent stale signals
	for {
		select {
		case <-wm.signals.VarSigChan:
			// Drain
		default:
			return
		}
	}
}
