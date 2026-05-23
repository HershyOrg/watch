package watch

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/HershyOrg/watch/api"
	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// watcherAPIServer provides HTTP API for Watcher monitoring and control.
type watcherAPIServer struct {
	watcher  *Watcher
	server   *http.Server
	handlers *api.WatcherAPIHandlers
}

// loggerAdapter adapts manager.Logger to api.LoggerInterface
type loggerAdapter struct {
	logger *manager.Logger
}

func (la *loggerAdapter) GetEffectLog() []interface{} {
	logs := la.logger.GetEffectLog()
	result := make([]interface{}, len(logs))
	for i, log := range logs {
		result[i] = log
	}
	return result
}

func (la *loggerAdapter) GetReduceLog() []interface{} {
	logs := la.logger.GetReduceLog()
	result := make([]interface{}, len(logs))
	for i, log := range logs {
		result[i] = log
	}
	return result
}

func (la *loggerAdapter) GetWatchErrorLog() []interface{} {
	logs := la.logger.GetWatchErrorLog()
	result := make([]interface{}, len(logs))
	for i, log := range logs {
		result[i] = log
	}
	return result
}

func (la *loggerAdapter) GetContextLog() []interface{} {
	logs := la.logger.GetContextLog()
	result := make([]interface{}, len(logs))
	for i, log := range logs {
		result[i] = log
	}
	return result
}

func (la *loggerAdapter) GetStateTransitionFaultLog() []interface{} {
	logs := la.logger.GetStateTransitionFaultLog()
	result := make([]interface{}, len(logs))
	for i, log := range logs {
		result[i] = log
	}
	return result
}

func (la *loggerAdapter) GetEffectResults() []interface{} {
	logs := la.logger.GetEffectResults()
	result := make([]interface{}, len(logs))
	for i, log := range logs {
		result[i] = log
	}
	return result
}

// signalsAdapter adapts Manager's signal queue info to api.SignalsInterface
type signalsAdapter struct {
	mgr *manager.Manager
}

func (sa *signalsAdapter) GetUserPending() int {
	userCount, _ := sa.mgr.GetSignalQueueLengths()
	return userCount
}

func (sa *signalsAdapter) GetControlPending() int {
	_, controlCount := sa.mgr.GetSignalQueueLengths()
	return controlCount
}

func (sa *signalsAdapter) PeekSignals(maxCount int) []api.SignalEntry {
	peekEntries := sa.mgr.PeekSignals(maxCount)
	entries := make([]api.SignalEntry, 0, len(peekEntries))
	for _, pe := range peekEntries {
		entries = append(entries, api.SignalEntry{
			Type:      pe.Type,
			Content:   pe.Content,
			CreatedAt: pe.CreatedAt,
		})
	}
	return entries
}

// managerAdapter adapts manager.Manager to api.ManagerInterface
type managerAdapter struct {
	manager *manager.Manager
}

func (ma *managerAdapter) GetWatchRegistry() api.WatchRegistryInterface {
	return &watchRegistryAdapter{registry: ma.manager.GetMachineRegistry()}
}

func (ma *managerAdapter) GetMemoCache() api.MemoCacheInterface {
	return &memoCacheAdapter{cache: ma.manager.GetMemoCache()}
}

// watchRegistryAdapter adapts wm.MachineRegistry
type watchRegistryAdapter struct {
	registry wm.MachineRegistry
}

func (wra *watchRegistryAdapter) GetAllVarNames() []string {
	if wra.registry == nil {
		return []string{}
	}
	machines := wra.registry.GetAllWatchMachines()
	varNames := make([]string, 0, len(machines))
	for _, m := range machines {
		varNames = append(varNames, m.VarName)
	}
	return varNames
}

// memoCacheAdapter adapts sync.Map memo cache
type memoCacheAdapter struct {
	cache *sync.Map
}

func (mca *memoCacheAdapter) GetAllEntries() map[string]interface{} {
	entries := make(map[string]interface{})
	mca.cache.Range(func(key, value interface{}) bool {
		if k, ok := key.(string); ok {
			entries[k] = value
		}
		return true
	})
	return entries
}

// varStateAdapter adapts manager.VarState to api.VarStateInterface
type varStateAdapter struct {
	state *manager.VarState
}

func (vsa *varStateAdapter) GetAll() map[string]interface{} {
	hvMap := vsa.state.GetAll()
	result := make(map[string]interface{}, len(hvMap))
	for k, hv := range hvMap {
		// Convert RawHershValue to interface{} for API compatibility
		if hv.Error != nil {
			result[k] = map[string]interface{}{
				"value": hv.Value,
				"error": hv.Error.Error(),
			}
		} else {
			result[k] = hv.Value
		}
	}
	return result
}

// configAdapter adapts shared.WatcherConfig to api.ConfigInterface
type configAdapter struct {
	config *shared.WatcherConfig
}

func (ca *configAdapter) GetServerPort() int {
	return ca.config.ServerPort
}

func (ca *configAdapter) GetSignalChanCapacity() int {
	return ca.config.SignalChanCapacity
}

func (ca *configAdapter) GetMaxLogEntries() int {
	return ca.config.MaxLogEntries
}

func (ca *configAdapter) GetMaxMemoEntries() int {
	return ca.config.MaxMemoEntries
}

// startAPIServer starts the HTTP API server (non-blocking).
func (w *Watcher) startAPIServer() (*watcherAPIServer, error) {
	if w.config.ServerPort == 0 {
		return nil, nil
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", w.config.ServerPort))
	if err != nil {
		return nil, err
	}

	// Create adapters
	loggerAdp := &loggerAdapter{logger: w.manager.GetLogger()}
	signalsAdp := &signalsAdapter{mgr: w.manager}
	managerAdp := &managerAdapter{manager: w.manager}
	varStateAdp := &varStateAdapter{state: w.manager.GetManagerState().VarState}
	configAdp := &configAdapter{config: &w.config}

	// Create handlers with closures
	handlers := api.NewWatcherAPIHandlers(
		func() (string, error) {
			state, err := w.State()
			if err != nil {
				return "", err
			}
			return state.String(), nil
		},
		func() bool {
			return w.isRunning.Load()
		},

		func() api.LoggerInterface {
			return loggerAdp
		},
		func() api.SignalsInterface {
			return signalsAdp
		},
		func(content string) error {
			return w.SendMessage(content)
		},
		func() api.ManagerInterface {
			return managerAdp
		},
		func() api.VarStateInterface {
			return varStateAdp
		},
		func() api.ConfigInterface {
			return configAdp
		},
	)

	mux := http.NewServeMux()

	// Register routes
	mux.HandleFunc("GET /watcher/status", handlers.HandleStatus)
	mux.HandleFunc("GET /watcher/logs", handlers.HandleLogs)
	mux.HandleFunc("GET /watcher/signals", handlers.HandleSignals)
	mux.HandleFunc("POST /watcher/message", handlers.HandleMessage)
	mux.HandleFunc("GET /watcher/watching", handlers.HandleWatching)
	mux.HandleFunc("GET /watcher/memoCache", handlers.HandleMemoCache)
	mux.HandleFunc("GET /watcher/varState", handlers.HandleVarState)
	mux.HandleFunc("GET /watcher/config", handlers.HandleConfig)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", w.config.ServerPort),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	apiServer := &watcherAPIServer{
		watcher:  w,
		server:   server,
		handlers: handlers,
	}

	// Start server in background goroutine
	go func() {
		fmt.Printf("[WatcherAPI] Starting HTTP server on :%d\n", w.config.ServerPort)
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[WatcherAPI] Server error: %v\n", err)
		}
	}()

	return apiServer, nil
}

// Shutdown gracefully shuts down the API server
func (s *watcherAPIServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}

	fmt.Println("[WatcherAPI] Shutting down HTTP server...")
	return s.server.Shutdown(ctx)
}

// Close immediately closes the API server without waiting for connections
func (s *watcherAPIServer) Close() error {
	if s == nil || s.server == nil {
		return nil
	}

	fmt.Println("[WatcherAPI] Force closing HTTP server...")
	return s.server.Close()
}
