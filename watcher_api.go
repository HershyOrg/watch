package watch

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/HershyOrg/watch/api"
	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
)

// WatcherAPIServer provides HTTP API for Watcher monitoring and control
type WatcherAPIServer struct {
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

// signalsAdapter adapts manager.SignalChannels to api.SignalsInterface
type signalsAdapter struct {
	signals *manager.SignalChannels
}

func (sa *signalsAdapter) GetVarSigCount() int {
	return len(sa.signals.VarSigChan)
}

func (sa *signalsAdapter) GetUserSigCount() int {
	return len(sa.signals.UserEventChan)
}

func (sa *signalsAdapter) GetManagerSigCount() int {
	return len(sa.signals.ControlEventChan)
}

func (sa *signalsAdapter) PeekSignals(maxCount int) []api.SignalEntry {
	entries := []api.SignalEntry{}

	// Peek from each channel (non-blocking read and write back)
	varCount := len(sa.signals.VarSigChan)
	userCount := len(sa.signals.UserEventChan)
	watcherCount := len(sa.signals.ControlEventChan)

	// Distribute maxCount across channels proportionally
	varLimit := min(varCount, maxCount/3)
	userLimit := min(userCount, maxCount/3)
	watcherLimit := min(watcherCount, maxCount/3)

	// Peek VarSig
	for i := 0; i < varLimit && i < varCount; i++ {
		select {
		case sig := <-sa.signals.VarSigChan:
			entries = append(entries, api.SignalEntry{
				Type:      "var",
				Content:   sig.String(),
				CreatedAt: sig.CreatedAt(),
			})
			// Write back to preserve the signal
			sa.signals.VarSigChan <- sig
		default:
			break
		}
	}

	// Peek UserSig
	for i := 0; i < userLimit && i < userCount; i++ {
		select {
		case sig := <-sa.signals.UserEventChan:
			entries = append(entries, api.SignalEntry{
				Type:      "user",
				Content:   sig.String(),
				CreatedAt: sig.CreatedAt(),
			})
			sa.signals.UserEventChan <- sig
		default:
			break
		}
	}

	// Peek WatcherSig
	for i := 0; i < watcherLimit && i < watcherCount; i++ {
		select {
		case sig := <-sa.signals.ControlEventChan:
			entries = append(entries, api.SignalEntry{
				Type:      "watcher",
				Content:   sig.String(),
				CreatedAt: sig.CreatedAt(),
			})
			sa.signals.ControlEventChan <- sig
		default:
			break
		}
	}

	return entries
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// managerAdapter adapts manager.Manager to api.ManagerInterface
type managerAdapter struct {
	manager *manager.Manager
}

func (ma *managerAdapter) GetWatchRegistry() api.WatchRegistryInterface {
	return &watchRegistryAdapter{registry: ma.manager.GetWatchRegistry()}
}

func (ma *managerAdapter) GetMemoCache() api.MemoCacheInterface {
	return &memoCacheAdapter{cache: ma.manager.GetMemoCache()}
}

// watchRegistryAdapter adapts sync.Map watch registry
type watchRegistryAdapter struct {
	registry *sync.Map
}

func (wra *watchRegistryAdapter) GetAllVarNames() []string {
	varNames := []string{}
	wra.registry.Range(func(key, value interface{}) bool {
		if handle, ok := value.(manager.WatchHandle); ok {
			varNames = append(varNames, handle.GetVarName())
		}
		return true
	})
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

// StartAPIServer starts the HTTP API server (non-blocking)
func (w *Watcher) StartAPIServer() (*WatcherAPIServer, error) {
	if w.config.ServerPort == 0 {
		return nil, nil // API disabled
	}

	// Create adapters
	loggerAdp := &loggerAdapter{logger: w.manager.GetLogger()}
	signalsAdp := &signalsAdapter{signals: w.manager.GetSignals()}
	managerAdp := &managerAdapter{manager: w.manager}
	varStateAdp := &varStateAdapter{state: w.manager.GetState().VarState}
	configAdp := &configAdapter{config: &w.config}

	// Create handlers with closures
	handlers := api.NewWatcherAPIHandlers(
		func() string {
			return w.GetState().String()
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

	apiServer := &WatcherAPIServer{
		watcher:  w,
		server:   server,
		handlers: handlers,
	}

	// Start server in background goroutine
	go func() {
		fmt.Printf("[WatcherAPI] Starting HTTP server on :%d\n", w.config.ServerPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[WatcherAPI] Server error: %v\n", err)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	return apiServer, nil
}

// Shutdown gracefully shuts down the API server
func (s *WatcherAPIServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}

	fmt.Println("[WatcherAPI] Shutting down HTTP server...")
	return s.server.Shutdown(ctx)
}

// Close immediately closes the API server without waiting for connections
func (s *WatcherAPIServer) Close() error {
	if s == nil || s.server == nil {
		return nil
	}

	fmt.Println("[WatcherAPI] Force closing HTTP server...")
	return s.server.Close()
}
