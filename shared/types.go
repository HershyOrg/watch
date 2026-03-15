// Package core contains shared types used across the hersh framework.
package shared

import (
	"context"
	"time"
)

// ManagerInnerState represents the internal state of the Manager.
// States form a finite state machine with specific transition rules.
// This is the core state that drives the reactive execution engine.
type ManagerInnerState uint8

const (
	// StateReady indicates the Manager is ready to execute on next signal
	StateReady ManagerInnerState = iota
	// StateRunning indicates the Manager is currently executing
	StateRunning
	// StateStopped indicates the Manager stopped normally and can be restarted
	StateStopped
	// StateKilled indicates the Manager was killed and is permanently stopped
	StateKilled
	// StateCrashed indicates the Manager crashed irrecoverably
	StateCrashed
	// StateWaitRecover indicates the Manager is waiting for recovery decision
	StateWaitRecover
)

func (s ManagerInnerState) String() string {
	switch s {
	case StateReady:
		return "Ready"
	case StateRunning:
		return "Running"
	case StateStopped:
		return "Stopped"
	case StateKilled:
		return "Killed"
	case StateCrashed:
		return "Crashed"
	case StateWaitRecover:
		return "WaitRecover"
	default:
		return "Unknown"
	}
}

// SignalPriority defines the priority order for signal processing.
// Lower numeric values indicate higher priority.
type SignalPriority uint8

const (
	// PriorityManagerInner is the highest priority (Watcher state control)
	PriorityManagerInner SignalPriority = 0
	// PriorityUser is medium priority (user messages)
	PriorityUser SignalPriority = 1
	// PriorityVar is the lowest priority (watched variable changes)
	PriorityVar SignalPriority = 2
)

// Signal is the interface that all signal types must implement.
// Signals trigger state transitions in the Watcher.
type Signal interface {
	Priority() SignalPriority
	CreatedAt() time.Time
	String() string
}

// Message represents a user-sent message to a managed function.
type Message struct {
	Content    string
	IsConsumed bool
	ReceivedAt time.Time
}

// String returns the message content.
func (m *Message) String() string {
	if m == nil {
		return ""
	}
	return m.Content
}

// TriggeredSignal represents which signals triggered the current execution.
// This allows managed functions to determine what caused them to run.
type TriggeredSignal struct {
	// IsUserSig indicates whether a UserSig triggered this execution
	IsUserSig bool

	// IsWatcherSig indicates whether a WatcherSig triggered this execution
	IsWatcherSig bool

	// VarSigNames contains the names of variables whose VarSigs triggered this execution
	// Multiple variables can trigger simultaneously due to batch processing
	VarSigNames []string
}

// HasTrigger returns true if any signal was triggered.
func (ts *TriggeredSignal) HasTrigger() bool {
	if ts == nil {
		return false
	}
	return ts.IsUserSig || ts.IsWatcherSig || len(ts.VarSigNames) > 0
}

// HasVarTrigger checks if a specific variable signal was triggered.
func (ts *TriggeredSignal) HasVarTrigger(varName string) bool {
	if ts == nil {
		return false
	}
	for _, v := range ts.VarSigNames {
		if v == varName {
			return true
		}
	}
	return false
}

// ManageContext provides runtime context for managed functions.
// It includes cancellation, deadlines, and access to Watcher features.
// This is the base interface used by manager package.
type ManageContext interface {
	context.Context

	// Message returns the current user message (nil if none)
	Message() *Message

	// GetTriggeredSignal returns information about which signals triggered the current execution
	// Returns nil if no trigger information is available (e.g., during initialization)
	GetTriggeredSignal() *TriggeredSignal

	// GetValue retrieves a value stored in the context by key
	// Returns nil if the key does not exist
	// WARNING: Returns the actual stored value (not a copy)
	// Mutating returned pointers will affect the stored state
	GetValue(key string) any

	// SetValue stores a value in the context by key
	// This allows managed functions to maintain state across executions
	// The framework automatically tracks changes for monitoring
	SetValue(key string, value any)

	SetEnvVars(map[string]string)

	// UpdateValue provides a safe way to update context values
	// The updateFn receives a copy of the current value and returns the new value
	// This ensures immutability and proper change tracking
	// Returns the new value after update
	UpdateValue(key string, updateFn func(current any) any) any

	// GetWatcher returns the watcher reference
	// Returns any to avoid circular dependency with hersh package
	GetWatcher() any

	// GetEnv returns the environment variable value for the given key
	// The second return value (ok) is true if the key exists, false otherwise
	// Environment variables are immutable after Watcher initialization
	GetEnv(key string) (string, bool)
}

// WatcherConfig holds configuration for creating a new Watcher.
type WatcherConfig struct {
	// ScheduleInfo contains scheduling information for the Watcher
	ScheduleInfo string

	// UserInfo contains user identification and metadata
	UserInfo string

	// ServerPort is the port number for the Watcher server
	ServerPort int

	// DefaultTimeout is the default timeout for managed function execution
	DefaultTimeout time.Duration

	// RecoveryPolicy defines how the Watcher handles failures
	RecoveryPolicy RecoveryPolicy

	// Resource limit settings for long-running stability
	MaxLogEntries      int // Maximum log entries before circular buffer truncation (default: 50,000)
	MaxWatches         int // Maximum number of concurrent watches (default: 1,000)
	MaxMemoEntries     int // Maximum number of memo cache entries (default: 1,000)
	SignalChanCapacity int // Signal channel buffer capacity (default: 50,000)
}

// RecoveryPolicy defines fault tolerance behavior.
type RecoveryPolicy struct {
	// MinConsecutiveFailures before entering recovery mode (default: 3)
	// Failures below this threshold return to Ready immediately
	MinConsecutiveFailures int

	// MaxConsecutiveFailures before crashing (default: 6)
	MaxConsecutiveFailures int

	// BaseRetryDelay is the initial delay before retry in recovery mode (default: 5s)
	// Used when failures >= MinConsecutiveFailures (exponential backoff)
	BaseRetryDelay time.Duration

	// MaxRetryDelay caps the maximum retry delay (default: 5m)
	MaxRetryDelay time.Duration

	// LightweightRetryDelays defines backoff delays for errors below MinConsecutiveFailures.
	// Example: [15s, 30s, 60s] means:
	//   - 1st failure: wait 15s → Ready
	//   - 2nd failure: wait 30s → Ready
	//   - 3rd failure: wait 60s → Ready
	//   - 4th+ failure: WaitRecover (if >= MinConsecutiveFailures)
	// If nil or empty, no delay (legacy behavior for backward compatibility).
	LightweightRetryDelays []time.Duration
}

// DefaultRecoveryPolicy returns sensible defaults.
func DefaultRecoveryPolicy() RecoveryPolicy {
	return RecoveryPolicy{
		MinConsecutiveFailures: 3,
		MaxConsecutiveFailures: 6,
		BaseRetryDelay:         5 * time.Second,
		MaxRetryDelay:          5 * time.Minute,
		LightweightRetryDelays: []time.Duration{
			15 * time.Second, // 1st failure: 15s
			30 * time.Second, // 2nd failure: 30s
			60 * time.Second, // 3rd failure: 60s
		},
	}
}

// FlowValue represents a value or error from a WatchFlow channel (generic version).
type FlowValue[T any] struct {
	V          T     // Value (passed to user if E == nil)
	E          error // Error (logged internally, never exposed to user)
	SkipSignal bool  // If true, skip sending VarSig (default false = send signal)
}

// RawFlowValue is the internal non-generic version used by manager for storage.
type RawFlowValue struct {
	V          any   // Value stored as any
	E          error // Error
	SkipSignal bool  // Skip signal flag
}

// WatchValue represents a value or error from Watch variables (generic version).
// This allows users to work with type-safe values while internally using any.
type WatchValue[T any] struct {
	Value      T      // The actual value (type-safe)
	Error      error  // Error that occurred during computation (nil if no error)
	VarName    string // Name of the watched variable (empty if not from Watch)
	NotUpdated bool   // true if this is an initial value, false if actually updated
}

// IsError returns true if this HershValue contains an error.
func (wv WatchValue[T]) IsError() bool {
	return wv.Error != nil
}

// IsUpdated returns true if this value has been updated (not initial).
func (wv WatchValue[T]) IsUpdated() bool {
	return !wv.NotUpdated
}

// IsUpdatedValide returns true if !IsError && IsUpdated
func (wv WatchValue[T]) IsUpdatedValide() bool {
	return !wv.IsError() && !wv.NotUpdated
}

// Get returns the value and error separately (Go idiomatic pattern).
func (wv WatchValue[T]) Get() (T, error) {
	return wv.Value, wv.Error
}

// MustGet returns the value or panics if there's an error.
// Use this only when you're certain there won't be an error.
func (wv WatchValue[T]) MustGet() T {
	if wv.Error != nil {
		panic("HershValue contains error: " + wv.Error.Error())
	}
	return wv.Value
}

// GetOr returns the value if no error, otherwise returns the default value.
func (wv WatchValue[T]) GetOr(defaultVal T) T {
	if wv.Error != nil {
		return defaultVal
	}
	return wv.Value
}

// IsTriggered returns true if this variable was triggered in the current execution.
// Requires a valid ManageContext to check the TriggeredSignal.
// Returns false if VarName is empty or if no trigger information is available.
func (wv WatchValue[T]) IsTriggered(ctx ManageContext) bool {
	if wv.VarName == "" {
		return false // Not a watched variable
	}

	trigger := ctx.GetTriggeredSignal()
	if trigger == nil {
		return false
	}

	return trigger.HasVarTrigger(wv.VarName)
}

// ToRaw converts HershValue[T] to RawHershValue for internal storage.
func (wv WatchValue[T]) ToRaw() RawWatchValue {
	return RawWatchValue{
		Value:      any(wv.Value),
		Error:      wv.Error,
		VarName:    wv.VarName,
		NotUpdated: wv.NotUpdated,
	}
}

// WatchLoopErr는 WatchLoop 상에서 발생 가능한 에러 종류이다.
type WatchLoopErrKind int

const (
	GetCallHandleErr WatchLoopErrKind = iota
	ComputeCallHookErr

	GetFlowHandleErr
	ComputeFlowHookErr
)

// RawWatchValue is the internal non-generic version used by VarState for storage.
type RawWatchValue struct {
	ErrorKind  WatchLoopErrKind
	Value      any    // The actual value stored as any
	Error      error  // Error that occurred during computation
	VarName    string // Name of the watched variable
	NotUpdated bool   // true if this is an initial value, false if actually updated
}

// TickValue represents a time-based tick event with count tracking.
// Used by WatchTick to provide both timestamp and tick count information.
type TickValue struct {
	Time       time.Time // Current tick timestamp
	TickCount  int       // Total number of ticks occurred (starts from 1)
	VarName    string    // Name of the watched variable (empty if not from WatchTick)
	NotUpdated bool      // true if this is an initial value, false if actually ticked
}

// IsUpdated returns true if this tick has been updated (not initial).
func (tv TickValue) IsUpdated() bool {
	return !tv.NotUpdated
}

// IsInitial returns true if this is an initial tick value (not yet updated).
func (tv TickValue) IsInitial() bool {
	return tv.NotUpdated
}

// IsTriggered returns true if this ticker was triggered in the current execution.
// Requires a valid ManageContext to check the TriggeredSignal.
// Returns false if VarName is empty or if no trigger information is available.
func (tv TickValue) IsTriggered(ctx ManageContext) bool {
	if tv.VarName == "" {
		return false // Not a watched variable
	}

	trigger := ctx.GetTriggeredSignal()
	if trigger == nil {
		return false
	}

	return trigger.HasVarTrigger(tv.VarName)
}

// DefaultWatcherConfig returns default configuration.
func DefaultWatcherConfig() WatcherConfig {
	return WatcherConfig{
		ServerPort:         8080,
		DefaultTimeout:     1 * time.Minute,
		RecoveryPolicy:     DefaultRecoveryPolicy(),
		MaxLogEntries:      50_000,
		MaxWatches:         1_000,
		MaxMemoEntries:     1_000,
		SignalChanCapacity: 50_000,
	}
}
