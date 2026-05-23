// Package core contains shared types used across the hersh framework.
package shared

import (
	"context"
	"fmt"
	"time"
)

// ControlState represents the Manager's control intent over the MgrFuncRunner.
// SemanticEvent → Desired state → recursive Effect loop → Terminal state.
// LoopState 패턴과 동일하게 인터페이스로 정의.
// Terminal 상태는 TerminalCause를 포함하여 "왜 terminal인가"를 기록한다.
type ControlState interface {
	controlState() // marker
	IsTerminal() bool
	String() string
}

// --- TerminalCause: terminal 전이의 근거 ---

type TerminalCause string

const (
	CauseHealthCheck       TerminalCause = "HealthCheck"
	CauseUserStop          TerminalCause = "UserStop"
	CauseUserKill          TerminalCause = "UserKill"
	CauseRecoveryExhausted TerminalCause = "RecoveryExhausted"
	CauseManagedFuncSignal TerminalCause = "ManagedFuncSignal"
	CauseReducerPanic      TerminalCause = "ReducerPanic"
)

// --- Non-terminal states ---

type ControlIdle struct{}

func (s *ControlIdle) controlState()    {}
func (s *ControlIdle) IsTerminal() bool { return false }
func (s *ControlIdle) String() string   { return "Idle" }

type ControlRunDesired struct{}

func (s *ControlRunDesired) controlState()    {}
func (s *ControlRunDesired) IsTerminal() bool { return false }
func (s *ControlRunDesired) String() string   { return "RunDesired" }

type ControlStopDesired struct{}

func (s *ControlStopDesired) controlState()    {}
func (s *ControlStopDesired) IsTerminal() bool { return false }
func (s *ControlStopDesired) String() string   { return "StopDesired" }

type ControlKillDesired struct{}

func (s *ControlKillDesired) controlState()    {}
func (s *ControlKillDesired) IsTerminal() bool { return false }
func (s *ControlKillDesired) String() string   { return "KillDesired" }

type ControlRecoverDesired struct{}

func (s *ControlRecoverDesired) controlState()    {}
func (s *ControlRecoverDesired) IsTerminal() bool { return false }
func (s *ControlRecoverDesired) String() string   { return "RecoverDesired" }

// --- Terminal states (Cause 포함) ---

type ControlStopped struct{ Cause TerminalCause }

func (s *ControlStopped) controlState()    {}
func (s *ControlStopped) IsTerminal() bool { return true }
func (s *ControlStopped) String() string   { return fmt.Sprintf("Stopped(%s)", s.Cause) }

type ControlKilled struct{ Cause TerminalCause }

func (s *ControlKilled) controlState()    {}
func (s *ControlKilled) IsTerminal() bool { return true }
func (s *ControlKilled) String() string   { return fmt.Sprintf("Killed(%s)", s.Cause) }

type ControlCrashed struct{ Cause TerminalCause }

func (s *ControlCrashed) controlState()    {}
func (s *ControlCrashed) IsTerminal() bool { return true }
func (s *ControlCrashed) String() string   { return fmt.Sprintf("Crashed(%s)", s.Cause) }

// GetTerminalCause는 ControlState가 terminal일 때 Cause를 추출한다.
// Non-terminal이면 빈 문자열을 반환한다.
func GetTerminalCause(cs ControlState) TerminalCause {
	switch s := cs.(type) {
	case *ControlStopped:
		return s.Cause
	case *ControlKilled:
		return s.Cause
	case *ControlCrashed:
		return s.Cause
	default:
		return ""
	}
}

// --- Terminal severity ---

// CauseSeverity는 TerminalCause의 심각도를 반환한다.
// 높을수록 강한(되돌리기 어려운) 원인.
func CauseSeverity(c TerminalCause) int {
	switch c {
	case CauseHealthCheck:
		return 1
	case CauseRecoveryExhausted:
		return 2
	case CauseManagedFuncSignal:
		return 3
	case CauseUserStop:
		return 4
	case CauseUserKill:
		return 5
	case CauseReducerPanic:
		return 6
	default:
		return 0
	}
}

// TerminalStateSeverity는 terminal 상태의 심각도를 반환한다.
// Non-terminal이면 0.
func TerminalStateSeverity(cs ControlState) int {
	switch cs.(type) {
	case *ControlStopped:
		return 1
	case *ControlKilled:
		return 2
	case *ControlCrashed:
		return 3
	default:
		return 0
	}
}

// IsStrongerTerminal은 newState가 current보다 강한 terminal인지 판단한다.
// 상태 심각도 우선 비교, 동일하면 Cause 심각도 비교.
// 둘 다 terminal이어야 의미 있음. current가 non-terminal이면 항상 true.
func IsStrongerTerminal(current, newState ControlState) bool {
	curSev := TerminalStateSeverity(current)
	newSev := TerminalStateSeverity(newState)

	if curSev == 0 {
		return true // current가 non-terminal이면 new가 항상 stronger
	}
	if newSev == 0 {
		return false // new가 non-terminal이면 절대 stronger 아님
	}

	if newSev > curSev {
		return true
	}
	if newSev < curSev {
		return false
	}
	// 같은 상태 → Cause 심각도 비교
	return CauseSeverity(GetTerminalCause(newState)) > CauseSeverity(GetTerminalCause(current))
}

// --- ControlSignal: ManagedFunc의 제어 의도 리턴 타입 ---

// SignalKind represents the kind of control signal.
type SignalKind uint8

const (
	// SignalNone indicates no control intent (success or regular error)
	SignalNone SignalKind = iota
	// SignalStop requests graceful stop
	SignalStop
	// SignalKill requests permanent termination
	SignalKill
	// SignalCrash indicates an irrecoverable error
	SignalCrash
)

func (k SignalKind) String() string {
	switch k {
	case SignalNone:
		return "None"
	case SignalStop:
		return "Stop"
	case SignalKill:
		return "Kill"
	case SignalCrash:
		return "Crash"
	default:
		return "Unknown"
	}
}

// ControlSignal represents a control intent returned by ManagedFunc.
// signal이 제어 의도 + 이유를, error가 순수 에러를 각각 담는다.
type ControlSignal struct {
	Kind   SignalKind
	Reason string
}

func (s ControlSignal) String() string {
	if s.Reason == "" {
		return s.Kind.String()
	}
	return fmt.Sprintf("%s: %s", s.Kind, s.Reason)
}

// IsNone returns true if this signal has no control intent.
func (s ControlSignal) IsNone() bool {
	return s.Kind == SignalNone
}

// None creates a ControlSignal with no control intent.
func None() ControlSignal {
	return ControlSignal{Kind: SignalNone}
}

// Stop creates a ControlSignal requesting graceful stop.
func Stop(reason string) ControlSignal {
	return ControlSignal{Kind: SignalStop, Reason: reason}
}

// Kill creates a ControlSignal requesting permanent termination.
func Kill(reason string) ControlSignal {
	return ControlSignal{Kind: SignalKill, Reason: reason}
}

// Crash creates a ControlSignal indicating an irrecoverable error.
func Crash(reason string) ControlSignal {
	return ControlSignal{Kind: SignalCrash, Reason: reason}
}

// RunnerState represents the MgrFuncRunner's own actual execution state.
type RunnerState uint8

const (
	// RunnerIdle indicates the MgrFuncRunner is not doing anything
	RunnerIdle RunnerState = iota
	// RunnerRunning indicates the MgrFuncRunner is executing the ManagedFunc
	RunnerRunning
	// RunnerCleaningUp indicates the MgrFuncRunner is running cleanup
	RunnerCleaningUp
	// RunnerSleeping indicates the MgrFuncRunner is in backoff/delay
	RunnerSleeping
	// RunnerStopped indicates the MgrFuncRunner has stopped
	RunnerStopped
	// RunnerKilled indicates the MgrFuncRunner has been killed
	RunnerKilled
	// RunnerCrashed indicates the MgrFuncRunner has crashed
	RunnerCrashed
)

func (s RunnerState) String() string {
	switch s {
	case RunnerIdle:
		return "Idle"
	case RunnerRunning:
		return "Running"
	case RunnerCleaningUp:
		return "CleaningUp"
	case RunnerSleeping:
		return "Sleeping"
	case RunnerStopped:
		return "Stopped"
	case RunnerKilled:
		return "Killed"
	case RunnerCrashed:
		return "Crashed"
	default:
		return "Unknown"
	}
}

// SignalPriority defines the priority order for event processing.
// Lower numeric values indicate higher priority.
type SignalPriority uint8

const (
	// PriorityControl is the highest priority (control events)
	PriorityControl SignalPriority = 0
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
	// Frozen keys (injected via SetFrozenValues) cannot be overwritten
	SetValue(key string, value any)

	// UpdateValue provides a safe way to update context values
	// The updateFn receives a copy of the current value and returns the new value
	// This ensures immutability and proper change tracking
	// Frozen keys are protected and return current value without modification
	// Returns the new value after update
	UpdateValue(key string, updateFn func(current any) any) any

	// SetFrozenValues injects immutable values into the store (init only)
	SetFrozenValues(map[string]any)

	// GetWatcher returns the watcher reference
	// Returns any to avoid circular dependency with hersh package
	GetWatcher() any

	// GetMachineRegistry returns the MachineRegistry for WatchXXX functions.
	// Returns any to avoid circular dependency (실제로는 wm.MachineRegistry).
	GetMachineRegistry() any
}

// WatcherConfig holds configuration for creating a new Watcher.
type WatcherConfig struct {
	// ScheduleInfo contains scheduling information for the Watcher
	ScheduleInfo string

	// UserInfo contains user identification and metadata
	UserInfo string

	// ServerPort is the port number for the Watcher server
	ServerPort int

	// DisableAPIServer disables the HTTP API server.
	// ServerPort must still be a valid positive port so the config remains complete.
	DisableAPIServer bool

	// DefaultTimeout is the default timeout for managed function execution
	DefaultTimeout time.Duration

	// RecoveryPolicy defines how the Watcher handles failures
	RecoveryPolicy RecoveryPolicy

	// HealthCheckInterval is the interval for checking WatchMachine health.
	// If any WM is in a terminal/stopped state, Manager transitions to terminal.
	// Health check is required; this value must be positive.
	HealthCheckInterval time.Duration

	// ShutdownTimeout is the maximum time Run allows Stop to finish after ctx cancellation.
	ShutdownTimeout time.Duration

	// CleanupTimeout bounds Cleaner.ClearRun. Cleanup functions must still observe ctx.Done()
	// because Go cannot forcibly stop a goroutine that ignores cancellation.
	CleanupTimeout time.Duration

	// WatchMachineHistoryMaxLen bounds each WatchMachine's reduced snapshot history.
	WatchMachineHistoryMaxLen int

	// WatchMachineHistoryMaxDur bounds each WatchMachine's reduced snapshot history by age.
	WatchMachineHistoryMaxDur time.Duration

	// Resource limit settings for long-running stability
	MaxLogEntries      int // Maximum log entries before circular buffer truncation (default: 50,000)
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
	// Must be non-nil, non-empty, and contain only positive durations.
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

// --- 변환 헬퍼 ---

// RawToTyped converts RawWatchValue to WatchValue[T].
// prev.Value가 nil이면 T의 zero value 사용 (초기 호출).
// 타입 미스매치 시 panic.
func RawToTyped[T any](rv RawWatchValueWithName) WatchValue[T] {
	if rv.RawWatchValue.Value == nil {
		var zero T
		return WatchValue[T]{
			Value:      zero,
			Error:      rv.RawWatchValue.Error,
			NotUpdated: true,
			varName:    rv.VarName,
		}
	}

	v, ok := rv.RawWatchValue.Value.(T)
	if !ok {
		cause := fmt.Errorf(
			"expected %T, got %T",
			*new(T),
			rv.RawWatchValue.Value,
		)
		panic(NewWatchInitPanic(rv.VarName, "watch value type mismatch", cause))
	}

	return WatchValue[T]{
		Value:      v,
		Error:      rv.RawWatchValue.Error,
		NotUpdated: rv.RawWatchValue.NotUpdated,
		varName:    rv.VarName,
	}
}

// WatchValue represents a value or error from Watch variables (generic version).
// This allows users to work with type-safe values while internally using any.
type WatchValue[T any] struct {
	Value      T      // The actual value (type-safe)
	Error      error  // Error that occurred during computation (nil if no error)
	varName    string // Name of the watched variable (empty if not from Watch)
	NotUpdated bool   // true if this is an initial value, false if actually updated
}

// VarName을 통해 사용자는 WatchValue접근 가능.
func (wv WatchValue[T]) VarName() string {
	return wv.varName
}

// IsError returns true if this HershValue contains an error.
func (wv WatchValue[T]) IsError() bool {
	return wv.Error != nil
}

// IsUpdated returns true if this value has been updated (not initial).
func (wv WatchValue[T]) IsUpdated() bool {
	return !wv.NotUpdated
}

// IsUpdatedValid returns true if !IsError && IsUpdated
func (wv WatchValue[T]) IsUpdatedValid() bool {
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
	if wv.VarName() == "" {
		return false // Not a watched variable
	}

	trigger := ctx.GetTriggeredSignal()

	if trigger == nil {
		return false
	}

	return trigger.HasVarTrigger(wv.VarName())
}

// ToRaw converts HershValue[T] to RawHershValue for internal storage.
func (wv WatchValue[T]) ToRaw() RawWatchValue {
	return RawWatchValue{
		Value:      any(wv.Value),
		Error:      wv.Error,
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
	Value      any   // The actual value stored as any
	Error      error // Error that occurred during computation
	NotUpdated bool  // true if this is an initial value, false if actually updated
}

// RawWatchValueWithName는 이름이 있는 RawWatchValue임
type RawWatchValueWithName struct {
	RawWatchValue RawWatchValue
	VarName       string
}

// TickValue represents a time-based tick event with count tracking.
// Used by WatchTick to provide both timestamp and tick count information.
type TickValue struct {
	Time       time.Time // Current tick timestamp
	TickCount  int       // Total number of ticks occurred (starts from 1)
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

// DefaultWatcherConfig returns default configuration.
func DefaultWatcherConfig() WatcherConfig {
	return WatcherConfig{
		ServerPort:                8080,
		DefaultTimeout:            1 * time.Minute,
		RecoveryPolicy:            DefaultRecoveryPolicy(),
		HealthCheckInterval:       3 * time.Second,
		ShutdownTimeout:           5 * time.Minute,
		CleanupTimeout:            5 * time.Minute,
		WatchMachineHistoryMaxLen: 30000,
		WatchMachineHistoryMaxDur: 24 * time.Hour,
		MaxLogEntries:             50_000,
		MaxMemoEntries:            1_000,
		SignalChanCapacity:        50_000,
	}
}
