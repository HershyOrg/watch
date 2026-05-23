package main

import (
	"context"
	"fmt"
	"time"

	"github.com/HershyOrg/watch"
)

// Simple counter example demonstrating hersh reactive framework
func main2() {
	fmt.Printf("=== Hersh Reactive Framework Demo ===\n")

	config := watch.DefaultWatcherConfig()
	config.DefaultTimeout = 5 * time.Second

	watcher := watch.NewWatcher(config)

	// Managed function with reactive state
	counter := 0
	managedFunc := func(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
		counter++
		fmt.Printf("[Execution %d]\n", counter)

		// Use Memo for expensive computation (cached)
		expensiveResult := watch.Memo(func() string {
			fmt.Println("  Computing expensive value...")
			time.Sleep(100 * time.Millisecond)
			return "cached_result"
		}, "expensiveComputation", ctx)
		fmt.Printf("  Memo result: %v\n", expensiveResult)

		// Use context value for shared state
		totalRuns := ctx.GetValue("totalRuns")
		if totalRuns == nil {
			totalRuns = 0
		}
		newTotal := totalRuns.(int) + 1
		ctx.SetValue("totalRuns", newTotal)
		fmt.Printf("  Total runs across executions: %d\n", newTotal)

		// Handle user messages
		if msg != nil {
			fmt.Printf("  Received message: '%s'\n", msg.Content)
			if msg.Content == "stop" {
				fmt.Println("  Stopping watcher gracefully...")
				return watch.Stop("user requested stop"), nil
			}
		}

		fmt.Println()
		return watch.None(), nil
	}

	// Register managed function with cleanup (no envVars needed)
	watcher.Manage(managedFunc, "counterExample", nil).Cleanup(func(ctx watch.ManageContext) {
		fmt.Println("\n[Cleanup] Watcher is shutting down")
		fmt.Printf("[Cleanup] Final state - Counter: %d\n", counter)
	})

	// Start watcher
	fmt.Println("Starting watcher...")
	if err := watcher.Start(); err != nil {
		panic(err)
	}

	// Wait for initialization
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("Watcher state: %s\n\n", watcherStateString(watcher))

	// Send messages to trigger executions
	fmt.Println("Sending message 1...")
	watcher.SendMessage("Hello from main!")
	time.Sleep(300 * time.Millisecond)

	fmt.Println("Sending message 2...")
	watcher.SendMessage("Another message")
	time.Sleep(300 * time.Millisecond)

	fmt.Println("Sending stop message...")
	watcher.SendMessage("stop")
	time.Sleep(300 * time.Millisecond)

	// Print logger summary
	fmt.Println("\n=== Execution Summary ===")
	printWatcherSummary(watcher)

	// Stop watcher
	if err := watcher.Stop(context.Background()); err != nil {
		fmt.Printf("Error stopping watcher: %v\n", err)
	}

	fmt.Println("\n=== Demo Complete ===")
}
