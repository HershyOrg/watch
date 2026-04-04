package shared

import "fmt"

// WatchInitPanic signals a critical panic during Watch initialization.
// This should immediately transition to Crashed state without recovery attempts.
type WatchInitPanic struct {
	VarName string
	Reason  string
	Cause   error
}

func (e *WatchInitPanic) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("WatchInitPanic[%s]: %s (caused by: %v)", e.VarName, e.Reason, e.Cause)
	}
	return fmt.Sprintf("WatchInitPanic[%s]: %s", e.VarName, e.Reason)
}

func (e *WatchInitPanic) Unwrap() error {
	return e.Cause
}

// IsWatchInitPanic checks if the error is a WatchInitPanic.
func (e *WatchInitPanic) IsWatchInitPanic() bool {
	return true
}

// NewWatchInitPanic creates a new WatchInitPanic error.
func NewWatchInitPanic(varName, reason string, cause error) *WatchInitPanic {
	return &WatchInitPanic{
		VarName: varName,
		Reason:  reason,
		Cause:   cause,
	}
}
