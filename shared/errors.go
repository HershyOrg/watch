package shared

import "fmt"

// VarNotInitializedError indicates a watched variable is not yet initialized.
// This is expected during InitRun phase.
type VarNotInitializedError struct {
	VarName string
}

func (e *VarNotInitializedError) Error() string {
	return fmt.Sprintf("variable not initialized: %s", e.VarName)
}

// NewVarNotInitializedErr creates a new VarNotInitializedError.
func NewVarNotInitializedErr(varName string) *VarNotInitializedError {
	return &VarNotInitializedError{VarName: varName}
}

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
