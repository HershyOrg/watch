package main

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/HershyOrg/watch"
)

const expectedWatchVersion = "v0.1.0"

func main() {
	version := watchVersion()
	fmt.Printf("watch module version: %s\n", version)
	if version != expectedWatchVersion {
		panic(fmt.Sprintf("expected github.com/HershyOrg/watch %s, got %s", expectedWatchVersion, version))
	}

	config := watch.DefaultWatcherConfig()
	config.DisableAPIServer = true

	w, err := watch.NewWatcher(config)
	if err != nil {
		panic(err)
	}

	w.Manage(func(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
		tick := watch.WatchTick("version_demo_tick", 200*time.Millisecond, ctx)
		if tick.IsUpdatedValid() {
			fmt.Printf("tick %d at %s\n", tick.Value.TickCount, tick.Value.Time.Format(time.RFC3339Nano))
		}
		if tick.Value.TickCount >= 3 {
			return watch.Stop("version demo complete"), nil
		}
		return watch.None(), nil
	}, "version-demo", nil)

	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := w.Run(runCtx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("final state: %T\n", state)
}

func watchVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(unknown)"
	}

	for _, dep := range info.Deps {
		if dep.Path == "github.com/HershyOrg/watch" {
			if dep.Replace != nil {
				return dep.Replace.Version
			}
			return dep.Version
		}
	}
	return "(not found)"
}
