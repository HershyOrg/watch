package watch

import (
	"time"

	"github.com/HershyOrg/watch/shared"
)

// WatchTick provides a convenient way to create a tick-based watcher.
// It automatically uses the current time as the initial value.
//
// Stub: pending reimplementation with WatchMachine.
func WatchTick(
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[shared.TickValue] {
	// TODO: Reimplement using WatchMachine in next phase
	panic("WatchTick: stub - pending reimplementation with WatchMachine")
}
