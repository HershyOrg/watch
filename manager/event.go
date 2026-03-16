package manager

import (
	"fmt"
	"time"

	"github.com/HershyOrg/watch/shared"
)

// --- SemanticEvent: queued in channels, fact-based ---

// ControlEventKind categorizes external control events.
type ControlEventKind uint8

const (
	StopRequested ControlEventKind = iota
	KillRequested
	RunRequested // for restart scenarios (Watcher.RunManager)
)

func (k ControlEventKind) String() string {
	switch k {
	case StopRequested:
		return "StopRequested"
	case KillRequested:
		return "KillRequested"
	case RunRequested:
		return "RunRequested"
	default:
		return "Unknown"
	}
}

// UserMessageReceived represents the fact that a user sent a message.
// Replaces the old UserSentMsgSig.
type UserMessageReceived struct {
	ReceivedTime time.Time
	UserMessage  *shared.Message
}

func (e *UserMessageReceived) Priority() shared.SignalPriority {
	return shared.PriorityUser
}

func (e *UserMessageReceived) CreatedAt() time.Time {
	return e.ReceivedTime
}

func (e *UserMessageReceived) String() string {
	msgContent := ""
	if e.UserMessage != nil {
		msgContent = e.UserMessage.Content
	}
	return fmt.Sprintf("UserMessageReceived{msg=%s, time=%s}",
		msgContent, e.ReceivedTime.Format(time.RFC3339))
}

// ControlEvent represents an external control request as a fact.
// Replaces the old ManagerInnerSig for external use (Watcher.Stop, RunManager, etc.).
type ControlEvent struct {
	ReceivedTime time.Time
	Kind         ControlEventKind
	NeedInit     bool   // Only used for RunRequested (restart scenarios)
	Reason       string
}

func (e *ControlEvent) Priority() shared.SignalPriority {
	return shared.PriorityControl
}

func (e *ControlEvent) CreatedAt() time.Time {
	return e.ReceivedTime
}

func (e *ControlEvent) String() string {
	return fmt.Sprintf("ControlEvent{kind=%s, reason=%s, time=%s}",
		e.Kind, e.Reason, e.ReceivedTime.Format(time.RFC3339))
}

// --- EffectDrivenEvent: returned by MgrFuncRunner, processed recursively ---

// EffectDrivenEvent is returned by MgrFuncRunner after executing an Effect.
// Processed recursively by the Reducer (never queued in channels).
type EffectDrivenEvent interface {
	isEffectDrivenEvent() // marker method
	String() string
}

// ExecutionCompleted indicates the ManagedFunc completed successfully.
type ExecutionCompleted struct{}

func (e *ExecutionCompleted) isEffectDrivenEvent() {}
func (e *ExecutionCompleted) String() string       { return "ExecutionCompleted" }

// ExecutionFailed indicates the ManagedFunc failed with consecutive failures >= MinConsecutiveFailures.
type ExecutionFailed struct {
	Err      error
	Failures int // consecutive failure count
}

func (e *ExecutionFailed) isEffectDrivenEvent() {}
func (e *ExecutionFailed) String() string {
	return fmt.Sprintf("ExecutionFailed{err=%v, failures=%d}", e.Err, e.Failures)
}

// ErrorSuppressed indicates a lightweight retry was applied (failures < MinConsecutiveFailures).
// MgrFuncRunner returns to Idle after backoff delay.
type ErrorSuppressed struct{}

func (e *ErrorSuppressed) isEffectDrivenEvent() {}
func (e *ErrorSuppressed) String() string       { return "ErrorSuppressed" }

// CleanupCompleted indicates cleanup finished for a terminal state transition.
type CleanupCompleted struct {
	ForState shared.ControlState // which terminal state this cleanup was for
}

func (e *CleanupCompleted) isEffectDrivenEvent() {}
func (e *CleanupCompleted) String() string {
	return fmt.Sprintf("CleanupCompleted{forState=%s}", e.ForState)
}

// RecoveryReady indicates the MgrFuncRunner is ready to retry after backoff.
type RecoveryReady struct{}

func (e *RecoveryReady) isEffectDrivenEvent() {}
func (e *RecoveryReady) String() string       { return "RecoveryReady" }

// RecoveryExhausted indicates max consecutive failures reached.
type RecoveryExhausted struct{}

func (e *RecoveryExhausted) isEffectDrivenEvent() {}
func (e *RecoveryExhausted) String() string       { return "RecoveryExhausted" }

// DirectKilled indicates the MgrFuncRunner was killed without cleanup.
type DirectKilled struct{}

func (e *DirectKilled) isEffectDrivenEvent() {}
func (e *DirectKilled) String() string       { return "DirectKilled" }

// DirectCrashed indicates the MgrFuncRunner crashed without cleanup.
type DirectCrashed struct{}

func (e *DirectCrashed) isEffectDrivenEvent() {}
func (e *DirectCrashed) String() string       { return "DirectCrashed" }
