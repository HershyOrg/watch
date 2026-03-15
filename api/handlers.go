package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// WatcherAPIHandlers provides HTTP handlers for WatcherServer API
type WatcherAPIHandlers struct {
	// Using interface{} to avoid circular dependency with hersh package
	getState    func() string
	isRunning   func() bool
	getLogger   func() LoggerInterface
	getSignals  func() SignalsInterface
	sendMessage func(string) error
	getManager  func() ManagerInterface
	getVarState func() VarStateInterface
	getConfig   func() ConfigInterface
	startTime   time.Time
}

// LoggerInterface defines methods needed from manager.Logger
type LoggerInterface interface {
	GetEffectLog() []interface{}
	GetReduceLog() []interface{}
	GetWatchErrorLog() []interface{}
	GetContextLog() []interface{}
	GetStateTransitionFaultLog() []interface{}
	GetEffectResults() []interface{}
}

// SignalsInterface defines methods needed from manager.SignalChannels
type SignalsInterface interface {
	GetVarSigCount() int
	GetUserSigCount() int
	GetManagerSigCount() int
	// For peek functionality
	PeekSignals(maxCount int) []SignalEntry
}

// ManagerInterface defines methods needed from manager.Manager
type ManagerInterface interface {
	GetWatchRegistry() WatchRegistryInterface
	GetMemoCache() MemoCacheInterface
}

// WatchRegistryInterface defines methods for accessing watch registry
type WatchRegistryInterface interface {
	GetAllVarNames() []string
}

// MemoCacheInterface defines methods for accessing memo cache
type MemoCacheInterface interface {
	GetAllEntries() map[string]interface{}
}

// VarStateInterface defines methods needed from manager.VarState
type VarStateInterface interface {
	GetAll() map[string]interface{}
}

// ConfigInterface defines methods needed from WatcherConfig
type ConfigInterface interface {
	GetServerPort() int
	GetSignalChanCapacity() int
	GetMaxLogEntries() int
	GetMaxMemoEntries() int
}

// NewWatcherAPIHandlers creates a new API handlers instance
func NewWatcherAPIHandlers(
	getState func() string,
	isRunning func() bool,
	getLogger func() LoggerInterface,
	getSignals func() SignalsInterface,
	sendMessage func(string) error,
	getManager func() ManagerInterface,
	getVarState func() VarStateInterface,
	getConfig func() ConfigInterface,
) *WatcherAPIHandlers {
	return &WatcherAPIHandlers{
		getState:    getState,
		isRunning:   isRunning,
		getLogger:   getLogger,
		getSignals:  getSignals,
		sendMessage: sendMessage,
		getManager:  getManager,
		getVarState: getVarState,
		getConfig:   getConfig,
		startTime:   time.Now(),
	}
}

// HandleStatus handles GET /watcher/status
func (h *WatcherAPIHandlers) HandleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	uptime := time.Since(h.startTime).String()

	response := StatusResponse{
		State:      h.getState(),
		IsRunning:  h.isRunning(),
		Uptime:     uptime,
		LastUpdate: time.Now(),
	}

	json.NewEncoder(w).Encode(response)
}

// HandleLogs handles GET /watcher/logs?type=X&limit=N
func (h *WatcherAPIHandlers) HandleLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	logType := r.URL.Query().Get("type")
	if logType == "" {
		logType = "all"
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	logger := h.getLogger()
	response := LogsResponse{}

	switch logType {
	case "reduce":
		logs := logger.GetReduceLog()
		response.ReduceLogs = h.limitReduceLogs(logs, limit)
	case "effect":
		logs := logger.GetEffectLog()
		response.EffectLogs = h.limitEffectLogs(logs, limit)
	case "effect_result":
		logs := logger.GetEffectResults()
		response.EffectResults = h.limitEffectResults(logs, limit)
	case "watch_error":
		logs := logger.GetWatchErrorLog()
		response.WatchErrorLogs = h.limitWatchErrorLogs(logs, limit)
	case "context":
		logs := logger.GetContextLog()
		response.ContextLogs = h.limitContextLogs(logs, limit)
	case "state_fault":
		logs := logger.GetStateTransitionFaultLog()
		response.StateFaultLogs = h.limitStateFaultLogs(logs, limit)
	case "all":
		response.EffectLogs = h.limitEffectLogs(logger.GetEffectLog(), limit)
		response.ReduceLogs = h.limitReduceLogs(logger.GetReduceLog(), limit)
		response.WatchErrorLogs = h.limitWatchErrorLogs(logger.GetWatchErrorLog(), limit)
		response.ContextLogs = h.limitContextLogs(logger.GetContextLog(), limit)
		response.StateFaultLogs = h.limitStateFaultLogs(logger.GetStateTransitionFaultLog(), limit)
		response.EffectResults = h.limitEffectResults(logger.GetEffectResults(), limit)
	default:
		http.Error(w, fmt.Sprintf("invalid log type: %s", logType), http.StatusBadRequest)
		return
	}

	json.NewEncoder(w).Encode(response)
}

// Helper functions to limit log entries
func (h *WatcherAPIHandlers) limitEffectLogs(logs []interface{}, limit int) []interface{} {
	if len(logs) > limit {
		return logs[len(logs)-limit:]
	}
	return logs
}

func (h *WatcherAPIHandlers) limitReduceLogs(logs []interface{}, limit int) []interface{} {
	if len(logs) > limit {
		return logs[len(logs)-limit:]
	}
	return logs
}

func (h *WatcherAPIHandlers) limitWatchErrorLogs(logs []interface{}, limit int) []interface{} {
	if len(logs) > limit {
		return logs[len(logs)-limit:]
	}
	return logs
}

func (h *WatcherAPIHandlers) limitContextLogs(logs []interface{}, limit int) []interface{} {
	if len(logs) > limit {
		return logs[len(logs)-limit:]
	}
	return logs
}

func (h *WatcherAPIHandlers) limitStateFaultLogs(logs []interface{}, limit int) []interface{} {
	if len(logs) > limit {
		return logs[len(logs)-limit:]
	}
	return logs
}

func (h *WatcherAPIHandlers) limitEffectResults(logs []interface{}, limit int) []interface{} {
	if len(logs) > limit {
		return logs[len(logs)-limit:]
	}
	return logs
}

// HandleSignals handles GET /watcher/signals
func (h *WatcherAPIHandlers) HandleSignals(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	signals := h.getSignals()

	varCount := signals.GetVarSigCount()
	userCount := signals.GetUserSigCount()
	managerCount := signals.GetManagerSigCount()

	// Peek recent signals (max 30)
	recentSignals := signals.PeekSignals(30)

	response := SignalsResponse{
		VarSigCount:     varCount,
		UserSigCount:    userCount,
		ManagerSigCount: managerCount,
		TotalPending:    varCount + userCount + managerCount,
		RecentSignals:   recentSignals,
		Timestamp:       time.Now(),
	}

	json.NewEncoder(w).Encode(response)
}

// HandleMessage handles POST /watcher/message
func (h *WatcherAPIHandlers) HandleMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req MessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errResp := ErrorResponse{Error: fmt.Sprintf("invalid request body: %v", err)}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errResp)
		return
	}

	if req.Content == "" {
		errResp := ErrorResponse{Error: "message content cannot be empty"}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(errResp)
		return
	}

	if err := h.sendMessage(req.Content); err != nil {
		errResp := ErrorResponse{Error: fmt.Sprintf("failed to send message: %v", err)}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(errResp)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "message sent"})
}

// HandleWatching handles GET /watcher/watching
func (h *WatcherAPIHandlers) HandleWatching(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	manager := h.getManager()
	watchRegistry := manager.GetWatchRegistry()
	watchedVars := watchRegistry.GetAllVarNames()

	response := WatchingResponse{
		WatchedVars: watchedVars,
		Count:       len(watchedVars),
		Timestamp:   time.Now(),
	}

	json.NewEncoder(w).Encode(response)
}

// HandleMemoCache handles GET /watcher/memoCache
func (h *WatcherAPIHandlers) HandleMemoCache(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	manager := h.getManager()
	memoCache := manager.GetMemoCache()
	entries := memoCache.GetAllEntries()

	response := MemoCacheResponse{
		Entries:   entries,
		Count:     len(entries),
		Timestamp: time.Now(),
	}

	json.NewEncoder(w).Encode(response)
}

// HandleVarState handles GET /watcher/varState
func (h *WatcherAPIHandlers) HandleVarState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	varState := h.getVarState()
	variables := varState.GetAll()

	response := VarStateResponse{
		Variables: variables,
		Count:     len(variables),
		Timestamp: time.Now(),
	}

	json.NewEncoder(w).Encode(response)
}

// HandleConfig handles GET /watcher/config
func (h *WatcherAPIHandlers) HandleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	config := h.getConfig()

	configData := WatcherConfigData{
		ServerPort:         config.GetServerPort(),
		SignalChanCapacity: config.GetSignalChanCapacity(),
		MaxLogEntries:      config.GetMaxLogEntries(),
		MaxMemoEntries:     config.GetMaxMemoEntries(),
	}

	response := ConfigResponse{
		Config:    configData,
		Timestamp: time.Now(),
	}

	json.NewEncoder(w).Encode(response)
}
