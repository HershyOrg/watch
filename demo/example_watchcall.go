package main

import (
	"fmt"
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// Advanced example demonstrating WatchCall reactive mechanism
func main1() {
	fmt.Println("=== Hersh WatchCall Demo ===")
	fmt.Println()

	config := watch.DefaultWatcherConfig()
	watcher := watch.NewWatcher(config)

	// Simulated external data source
	externalCounter := 0

	managedFunc := func(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
		fmt.Printf("\n[Managed Function Execution]\n")

		// WatchCall monitors external value and triggers re-execution on change
		hv := watch.WatchCall[int](
			0, // Initial counter value
			func(callCtx wm.CallContext) (func(runCtx wm.RunContext) wm.UpdateFunc[int], error) {
				return func(runCtx wm.RunContext) wm.UpdateFunc[int] {
					// Simulate polling external data source
					currentValue := externalCounter
					externalCounter++
					return func(prev shared.WatchValue[int]) (shared.WatchValue[int], bool) {
						fmt.Printf("  Polling: prev=%v, current=%v\n", prev.Value, currentValue)
						return shared.WatchValue[int]{Value: currentValue}, false
					}
				}, nil
			},
			"externalCounter",
			300*time.Millisecond, // Poll every 300ms
			ctx,
		)

		// React to the watched value
		if hv.Value == 0 {
			fmt.Println("  Status: Waiting for first value...")
		} else {
			counter := hv.Value // Type-safe, no assertion needed
			fmt.Printf("  Watched Value: %d\n", counter)

			// Business logic based on watched value
			if counter%3 == 0 && counter > 0 {
				fmt.Printf("  🎯 Milestone reached at %d!\n", counter)
			}

			// Stop condition
			if counter >= 5 {
				fmt.Println("  ✅ Target reached, stopping...")
				return watch.Stop("reached target count"), nil
			}
		}

		return watch.None(), nil
	}

	watcher.Manage(managedFunc, "watchCallExample", nil).Cleanup(func(ctx watch.ManageContext) {
		fmt.Println("\n[Cleanup] Shutting down watcher")
	})

	fmt.Println("Starting watcher...")
	err := watcher.StartAndRun()
	if err != nil {
		panic(err)
	}

	// Wait for reactive executions triggered by WatchCall
	time.Sleep(3 * time.Second)

	fmt.Printf("\nFinal state: %s\n", watcher.GetState())

	// Print summary
	fmt.Println("\n=== Execution Summary ===")
	watcher.GetLogger().PrintSummary()

	err = watcher.StopAll()
	if err != nil {
		fmt.Printf("Error stopping: %v\n", err)
	}

	fmt.Println("\n=== Demo Complete ===")
}
