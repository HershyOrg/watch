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

	// Error types
	ControlError           = shared.ControlError
	StopError              = shared.StopError
	KillError              = shared.KillError
	CrashError             = shared.CrashError
	VarNotInitializedError = shared.VarNotInitializedError
)

// Re-export ControlState constants
const (
	ControlIdle           = shared.ControlIdle
	ControlRunDesired     = shared.ControlRunDesired
	ControlStopDesired    = shared.ControlStopDesired
	ControlKillDesired    = shared.ControlKillDesired
	ControlRecoverDesired = shared.ControlRecoverDesired
	ControlStopped        = shared.ControlStopped
	ControlKilled         = shared.ControlKilled
	ControlCrashed        = shared.ControlCrashed

	PriorityControl = shared.PriorityControl
	PriorityUser    = shared.PriorityUser
	PriorityVar     = shared.PriorityVar
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
