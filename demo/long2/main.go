package main

import (
	"fmt"
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/demo/logic"
)

const a = "a"

func main() {
	print(a)
	config := watch.DefaultWatcherConfig()
	config.DefaultTimeout = 2 * time.Minute
	watcher := watch.NewWatcher(config)
	watcher.Manage(logic.DelcaredLogic, "TradingSimulator", map[string]any{
		"DEMO_NAME": "Long-Running Trading Simulator", "DEMO_VERSION": "1.0.0",
	}).Cleanup(logic.CleanupReducer)

	fmt.Println("\n▶️  Press Ctrl+C to stop | Auto-stop after 10 minutes")
	result, err := watcher.StartAndWait(
		watch.WithTimeout(10*time.Minute),
		watch.WithInterrupt(),
	)
	if err != nil {
		fmt.Printf("❌ Failed: %v\n", err)
		return
	}

	switch result.Reason {
	case watch.WaitReasonTimeout:
		fmt.Println("\n⏰ Target duration reached")
	case watch.WaitReasonSignal:
		fmt.Println("\n🛑 Stopped gracefully")
	default:
		fmt.Printf("\nEnded: %s (%s)\n", result.State, result.Reason)
	}

	watcher.GetLogger().PrintSummary()
}
