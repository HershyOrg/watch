package manager

import (
	"fmt"
	"time"

	"github.com/HershyOrg/watch/shared"
)

// Effect defines an action to be executed by the Target.
type Effect interface {
	effectMarker() // marker method
	String() string
}

// RunEffect tells the Target to execute the ManagedFunc.
type RunEffect struct {
	TriggeredSignal *shared.TriggeredSignal
	NeedInit        bool // Whether initialization is needed (restart scenarios)
}

func (e *RunEffect) effectMarker() {}
func (e *RunEffect) String() string {
	if e.NeedInit {
		return "RunEffect{init=true}"
	}
	return "RunEffect"
}

// CleanupEffect tells the Target to run cleanup for a terminal state transition.
type CleanupEffect struct {
	ForState shared.ControlState // which terminal state this cleanup is for
}

func (e *CleanupEffect) effectMarker() {}
func (e *CleanupEffect) String() string {
	return fmt.Sprintf("CleanupEffect{forState=%s}", e.ForState)
}

// RecoverEffect tells the Target to attempt recovery.
type RecoverEffect struct{}

func (e *RecoverEffect) effectMarker() {}
func (e *RecoverEffect) String() string { return "RecoverEffect" }

// DirectKillEffect tells the Target to transition to Killed without cleanup.
type DirectKillEffect struct{}

func (e *DirectKillEffect) effectMarker() {}
func (e *DirectKillEffect) String() string { return "DirectKillEffect" }

// DirectCrashEffect tells the Target to transition to Crashed without cleanup.
type DirectCrashEffect struct{}

func (e *DirectCrashEffect) effectMarker() {}
func (e *DirectCrashEffect) String() string { return "DirectCrashEffect" }

// EffectResult represents the result of executing an effect.
type EffectResult struct {
	Effect    Effect
	Success   bool
	Error     error
	Timestamp time.Time
}

func (er *EffectResult) String() string {
	status := "Ok"
	if !er.Success {
		status = fmt.Sprintf("Err(%v)", er.Error)
	}
	return fmt.Sprintf("EffectResult{effect=%s, status=%s, time=%s}",
		er.Effect, status, er.Timestamp.Format(time.RFC3339))
}
