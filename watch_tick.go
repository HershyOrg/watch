package watch

import (
	"time"

	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// WatchTick provides a convenient way to create a tick-based watcher.
// It automatically uses the current time as the initial value.
// TickCount is the number of ticks emitted by the WatchMachine for this varName.
//
//go:noinline
func WatchTick(
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[shared.TickValue] {
	init := shared.TickValue{Time: time.Now(), NotUpdated: true}
	contract := watchContract[shared.TickValue](varName, wm.WatchCallType, watchRegistrationPC())

	return watchCallWithContract(
		init,
		func(callCtx wm.CallContext) (func(runCtx wm.RunContext) wm.UpdateFunc[shared.TickValue], error) {

			return func(runCtx wm.RunContext) wm.UpdateFunc[shared.TickValue] {

				return func(prev shared.WatchValue[shared.TickValue]) (shared.WatchValue[shared.TickValue], bool) {
					return shared.WatchValue[shared.TickValue]{
						Value: shared.TickValue{
							Time:      time.Now(),
							TickCount: prev.Value.TickCount + 1,
						},
					}, false
				}
			}, nil
		},
		varName,
		tick,
		runCtx,
		contract,
	)
}
