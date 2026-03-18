package util

import (
	"time"

	"github.com/HershyOrg/watch/shared"
)

// WatchTick monitors time-based intervals and returns a HershTick with timestamp and count.
//
// Stub: pending reimplementation with WatchMachine.
func WatchTick(varName string, tickInterval time.Duration, runCtx shared.ManageContext) shared.TickValue {
	// TODO: Reimplement using WatchMachine in next phase
	panic("WatchTick: stub - pending reimplementation with WatchMachine")
}
