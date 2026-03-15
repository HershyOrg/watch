// Package api provides HTTP API types and handlers for WatcherServer.
package api

import (
	"time"
)

// StatusResponse represents the response for GET /watcher/status
type StatusResponse struct {
	State      string    `json:"state"`
	IsRunning  bool      `json:"isRunning"`
	Uptime     string    `json:"uptime"`
	LastUpdate time.Time `json:"lastUpdate"`
}

// LogsResponse represents the response for GET /watcher/logs
type LogsResponse struct {
	EffectLogs     []interface{} `json:"effectLogs,omitempty"`
	ReduceLogs     []interface{} `json:"reduceLogs,omitempty"`
	WatchErrorLogs []interface{} `json:"watchErrorLogs,omitempty"`
	ContextLogs    []interface{} `json:"contextLogs,omitempty"`
	StateFaultLogs []interface{} `json:"stateFaultLogs,omitempty"`
	EffectResults  []interface{} `json:"effectResults,omitempty"`
}

// SignalsResponse represents the response for GET /watcher/signals
type SignalsResponse struct {
	VarSigCount     int           `json:"varSigCount"`
	UserSigCount    int           `json:"userSigCount"`
	ManagerSigCount int           `json:"managerSigCount"`
	TotalPending    int           `json:"totalPending"`
	RecentSignals   []SignalEntry `json:"recentSignals"` // Recent signals (max 30)
	Timestamp       time.Time     `json:"timestamp"`
}

// SignalEntry represents a single signal entry
type SignalEntry struct {
	Type      string    `json:"type"`      // "var", "user", "watcher"
	Content   string    `json:"content"`   // Signal string representation
	CreatedAt time.Time `json:"createdAt"` // Signal creation time
}

// MessageRequest represents the request body for POST /watcher/message
type MessageRequest struct {
	Content string `json:"content"`
}

// WatchingResponse represents the response for GET /watcher/watching
type WatchingResponse struct {
	WatchedVars []string  `json:"watchedVars"` // List of watched variable names
	Count       int       `json:"count"`
	Timestamp   time.Time `json:"timestamp"`
}

// MemoCacheResponse represents the response for GET /watcher/memoCache
type MemoCacheResponse struct {
	Entries   map[string]interface{} `json:"entries"` // Memo cache key-value pairs
	Count     int                    `json:"count"`
	Timestamp time.Time              `json:"timestamp"`
}

// VarStateResponse represents the response for GET /watcher/varState
type VarStateResponse struct {
	Variables map[string]interface{} `json:"variables"` // Current variable state snapshot
	Count     int                    `json:"count"`
	Timestamp time.Time              `json:"timestamp"`
}

// ConfigResponse represents the response for GET /watcher/config
type ConfigResponse struct {
	Config    WatcherConfigData `json:"config"`
	Timestamp time.Time         `json:"timestamp"`
}

// WatcherConfigData holds WatcherConfig field values
type WatcherConfigData struct {
	ServerPort         int `json:"serverPort"`
	SignalChanCapacity int `json:"signalChanCapacity"`
	MaxLogEntries      int `json:"maxLogEntries"`
	MaxMemoEntries     int `json:"maxMemoEntries"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}
