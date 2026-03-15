package shared

import "fmt"

// ControlError represents errors that control Watcher state transitions.
type ControlError interface {
	error
	IsControlError() bool
}

// StopError signals the Watcher should stop gracefully.
// The Watcher can be restarted with InitRun.
type StopError struct {
	Reason string
}

func (e *StopError) Error() string {
	return fmt.Sprintf("stop: %s", e.Reason)
}

func (e *StopError) IsControlError() bool {
	return true
}

// NewStopErr creates a new StopError.
func NewStopErr(reason string) *StopError {
	return &StopError{Reason: reason}
}

// KillError signals the Watcher should terminate permanently.
// The Watcher cannot be restarted.
type KillError struct {
	Reason string
}

func (e *KillError) Error() string {
	return fmt.Sprintf("kill: %s", e.Reason)
}

func (e *KillError) IsControlError() bool {
	return true
}

// NewKillErr creates a new KillError.
func NewKillErr(reason string) *KillError {
	return &KillError{Reason: reason}
}

// CrashError signals an irrecoverable error occurred.
// The Watcher enters Crashed state and cannot recover.
type CrashError struct {
	Reason string
	Cause  error
}

func (e *CrashError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("crash: %s (caused by: %v)", e.Reason, e.Cause)
	}
	return fmt.Sprintf("crash: %s", e.Reason)
}

func (e *CrashError) IsControlError() bool {
	return true
}

func (e *CrashError) Unwrap() error {
	return e.Cause
}

// NewCrashErr creates a new CrashError.
func NewCrashErr(reason string, cause error) *CrashError {
	return &CrashError{Reason: reason, Cause: cause}
}

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

func (e *WatchInitPanic) IsControlError() bool {
	return true
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
