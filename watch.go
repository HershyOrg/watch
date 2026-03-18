package watch

import (
	"context"
	"time"

	"github.com/HershyOrg/watch/manager"
	"github.com/HershyOrg/watch/shared"
)

// getManagerFromContext extracts the Manager from ManageContext.
func getManagerFromContext(ctx shared.ManageContext) *manager.Manager {
	// ManageContext is now *manager.ManageContext
	if mc, ok := ctx.(*manager.ManageContext); ok {
		return mc.GetManager()
	}
	return nil
}

// DELELTED_WatchCall is a stub. The old implementation relied on deleted types
// (wm.DELETED_VarUpdateFunc, wm.DELETED_VarSig, manager.TickHandle, manager.RegisterWatch, etc.).
// It will be replaced by a new WatchCall implementation using WatchMachine.
func DELELTED_WatchCall[T any](
	init T,
	getComputationFunc func() (func(T) (T, error), bool, error),
	varName string,
	tick time.Duration,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	// TODO: Reimplement using WatchMachine in next phase
	panic("DELELTED_WatchCall: stub - pending reimplementation with WatchMachine")
}

// tickWatchLoop is a stub. The old implementation relied on deleted types.
func tickWatchLoop(mgr *manager.Manager, varName string, ctx context.Context) {
	// TODO: Reimplement using WatchMachine in next phase
	panic("tickWatchLoop: stub - pending reimplementation with WatchMachine")
}

// DELETED_WatchFlow is a stub. The old implementation relied on deleted types
// (wm.DELETED_VarSig, manager.FlowHandle, manager.RegisterWatch, etc.).
// It will be replaced by a new WatchFlow implementation using WatchMachine.
func DELETED_WatchFlow[T any](
	init T,
	getChannelFunc func(ctx context.Context) (<-chan shared.FlowValue[T], error),
	varName string,
	runCtx shared.ManageContext,
) shared.WatchValue[T] {
	// TODO: Reimplement using WatchMachine in next phase
	panic("DELETED_WatchFlow: stub - pending reimplementation with WatchMachine")
}

// flowWatchLoop is a stub. The old implementation relied on deleted types.
func flowWatchLoop(mgr *manager.Manager, varName string, ctx context.Context, sourceChan <-chan shared.RawFlowValue) {
	// TODO: Reimplement using WatchMachine in next phase
	panic("flowWatchLoop: stub - pending reimplementation with WatchMachine")
}
