package watch

import (
	"sync/atomic"
	"time"

	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// WatchTick provides a convenient way to create a tick-based watcher.
// It automatically uses the current time as the initial value.
func WatchTick(
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[shared.TickValue] {
	var tickCount int64

	init := shared.TickValue{Time: time.Now(), NotUpdated: true, VarName: varName}

	return DELELTED_WatchCall[shared.TickValue](
		init,
		func(callCtx wm.CallContext) (wm.CallHandle[shared.TickValue], error) {
			return wm.CallHandle[shared.TickValue]{
				Tick: tick,
				GetUpdateFunc: func(runCtx wm.RunContext) wm.UpdateFunc[shared.TickValue] {
					return func(prev shared.WatchValue[shared.TickValue]) (shared.WatchValue[shared.TickValue], bool) {
						count := atomic.AddInt64(&tickCount, 1)
						return shared.WatchValue[shared.TickValue]{
							Value: shared.TickValue{
								Time:      time.Now(),
								TickCount: int(count),
								VarName:   varName,
							},
							VarName: varName,
						}, false
					}
				},
			}, nil
		},
		varName,
		tick,
		runCtx,
	)
}
