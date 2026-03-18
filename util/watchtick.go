package util

import (
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"
)

// WatchTick monitors time-based intervals and returns a TickValue with timestamp and count.
func WatchTick(varName string, tickInterval time.Duration, runCtx shared.ManageContext) shared.TickValue {
	wv := watch.WatchTick(varName, tickInterval, runCtx)
	return wv.Value
}
