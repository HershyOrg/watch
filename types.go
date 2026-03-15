// Package hersh provides a reactive framework for Go with monitoring and control capabilities.
package watch

import (
	"github.com/HershyOrg/watch/shared"
)

// Re-export core types for convenience
type (
	ManagerInnerState = shared.ManagerInnerState
	SignalPriority    = shared.SignalPriority
	Signal            = shared.Signal
	ManageContext     = shared.ManageContext
	Message           = shared.Message
	WatcherConfig     = shared.WatcherConfig
	RecoveryPolicy    = shared.RecoveryPolicy
	// HershValue is now generic: use shared.HershValue[T] directly
	// FlowValue is now generic: use shared.FlowValue[T] directly

	// Error types
	ControlError           = shared.ControlError
	StopError              = shared.StopError
	KillError              = shared.KillError
	CrashError             = shared.CrashError
	VarNotInitializedError = shared.VarNotInitializedError
)

// VarUpdateFunc is a function that updates a variable's state.
// DEPRECATED: This type is no longer used. Use manager.VarUpdateFunc instead.
// It takes the previous state and returns the next state, a boolean indicating if the state changed, and an error.
type VarUpdateFunc func(prev any) (next any, changed bool, err error)

// Re-export constants
const (
	StateReady       = shared.StateReady
	StateRunning     = shared.StateRunning
	StateStopped     = shared.StateStopped
	StateKilled      = shared.StateKilled
	StateCrashed     = shared.StateCrashed
	StateWaitRecover = shared.StateWaitRecover

	PriorityManagerInner = shared.PriorityManagerInner
	PriorityUser         = shared.PriorityUser
	PriorityVar          = shared.PriorityVar
)

// Re-export functions
var (
	NewStopErr              = shared.NewStopErr
	NewKillErr              = shared.NewKillErr
	NewCrashErr             = shared.NewCrashErr
	NewVarNotInitializedErr = shared.NewVarNotInitializedErr
	DefaultRecoveryPolicy   = shared.DefaultRecoveryPolicy
	DefaultWatcherConfig    = shared.DefaultWatcherConfig
)
