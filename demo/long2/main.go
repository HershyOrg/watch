package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

	interruptCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	runCtx, cancel := context.WithTimeout(interruptCtx, 10*time.Minute)
	defer cancel()

	state, err := watcher.Run(runCtx)
	if err != nil {
		fmt.Printf("❌ Failed: %v\n", err)
		return
	}

	switch {
	case runCtx.Err() == context.DeadlineExceeded:
		fmt.Println("\n⏰ Target duration reached")
	case interruptCtx.Err() != nil:
		fmt.Println("\n🛑 Stopped gracefully")
	default:
		fmt.Printf("\nEnded: %s\n", state)
	}

	if logger, err := watcher.Logger(); err == nil && logger != nil {
		logger.PrintSummary()
	}
}
