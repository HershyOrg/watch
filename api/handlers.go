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
	getState    func() (string, error)
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
	GetUserPending() int
	GetControlPending() int
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
	getState func() (string, error),
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

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{Error: message})
}

// HandleStatus handles GET /watcher/status
func (h *WatcherAPIHandlers) HandleStatus(w http.ResponseWriter, r *http.Request) {
	state, err := h.getState()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get watcher state: %v", err))
		return
	}

	uptime := time.Since(h.startTime).String()

	response := StatusResponse{
		State:      state,
		IsRunning:  h.isRunning(),
		Uptime:     uptime,
		LastUpdate: time.Now(),
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleLogs handles GET /watcher/logs?type=X&limit=N
func (h *WatcherAPIHandlers) HandleLogs(w http.ResponseWriter, r *http.Request) {
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
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid log type: %s", logType))
		return
	}

	writeJSON(w, http.StatusOK, response)
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
	signals := h.getSignals()

	userPending := signals.GetUserPending()
	controlPending := signals.GetControlPending()

	// Peek recent signals (max 30)
	recentSignals := signals.PeekSignals(30)

	response := SignalsResponse{
		UserPending:    userPending,
		ControlPending: controlPending,
		RecentSignals:  recentSignals,
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleMessage handles POST /watcher/message
func (h *WatcherAPIHandlers) HandleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req MessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if req.Content == "" {
		writeJSONError(w, http.StatusBadRequest, "message content cannot be empty")
		return
	}

	if err := h.sendMessage(req.Content); err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to send message: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "message sent"})
}

// HandleWatching handles GET /watcher/watching
func (h *WatcherAPIHandlers) HandleWatching(w http.ResponseWriter, r *http.Request) {
	manager := h.getManager()
	watchRegistry := manager.GetWatchRegistry()
	watchedVars := watchRegistry.GetAllVarNames()

	response := WatchingResponse{
		WatchedVars: watchedVars,
		Count:       len(watchedVars),
		Timestamp:   time.Now(),
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleMemoCache handles GET /watcher/memoCache
func (h *WatcherAPIHandlers) HandleMemoCache(w http.ResponseWriter, r *http.Request) {
	manager := h.getManager()
	memoCache := manager.GetMemoCache()
	entries := memoCache.GetAllEntries()

	response := MemoCacheResponse{
		Entries:   entries,
		Count:     len(entries),
		Timestamp: time.Now(),
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleVarState handles GET /watcher/varState
func (h *WatcherAPIHandlers) HandleVarState(w http.ResponseWriter, r *http.Request) {
	varState := h.getVarState()
	variables := varState.GetAll()

	response := VarStateResponse{
		Variables: variables,
		Count:     len(variables),
		Timestamp: time.Now(),
	}

	writeJSON(w, http.StatusOK, response)
}

// HandleConfig handles GET /watcher/config
func (h *WatcherAPIHandlers) HandleConfig(w http.ResponseWriter, r *http.Request) {
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

	writeJSON(w, http.StatusOK, response)
}
