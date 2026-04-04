// Package hersh provides a reactive framework for Go with monitoring and control capabilities.
package watch

import (
	"github.com/HershyOrg/watch/shared"
)

// Re-export core types for convenience
type (
	ControlState = shared.ControlState
	TargetState  = shared.RunnerState

	SignalPriority = shared.SignalPriority
	Signal         = shared.Signal
	ManageContext  = shared.ManageContext
	Message        = shared.Message
	WatcherConfig  = shared.WatcherConfig
	RecoveryPolicy = shared.RecoveryPolicy

	// Signal types
	ControlSignal = shared.ControlSignal
	SignalKind    = shared.SignalKind

	// Error types
	VarNotInitializedError = shared.VarNotInitializedError

	// ControlState types (interface + structs)
	ControlIdle           = shared.ControlIdle
	ControlRunDesired     = shared.ControlRunDesired
	ControlStopDesired    = shared.ControlStopDesired
	ControlKillDesired    = shared.ControlKillDesired
	ControlRecoverDesired = shared.ControlRecoverDesired
	ControlStopped        = shared.ControlStopped
	ControlKilled         = shared.ControlKilled
	ControlCrashed        = shared.ControlCrashed

	// TerminalCause
	TerminalCause = shared.TerminalCause
)

// Re-export TerminalCause constants
const (
	CauseHealthCheck       = shared.CauseHealthCheck
	CauseUserStop          = shared.CauseUserStop
	CauseUserKill          = shared.CauseUserKill
	CauseRecoveryExhausted = shared.CauseRecoveryExhausted
	CauseManagedFuncSignal = shared.CauseManagedFuncSignal
	CauseReducerPanic      = shared.CauseReducerPanic
)

// Re-export SignalPriority constants
const (
	PriorityControl = shared.PriorityControl
	PriorityUser    = shared.PriorityUser
	PriorityVar     = shared.PriorityVar
)

// Re-export ControlSignal constants
const (
	SignalNone  = shared.SignalNone
	SignalStop  = shared.SignalStop
	SignalKill  = shared.SignalKill
	SignalCrash = shared.SignalCrash
)

// Re-export ControlSignal constructors and functions
var (
	None  = shared.None
	Stop  = shared.Stop
	Kill  = shared.Kill
	Crash = shared.Crash

	NewVarNotInitializedErr = shared.NewVarNotInitializedErr
	DefaultRecoveryPolicy   = shared.DefaultRecoveryPolicy
	DefaultWatcherConfig    = shared.DefaultWatcherConfig
)
