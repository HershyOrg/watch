package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"

	"github.com/HershyOrg/watch/wm"
)

const (
	// Demo configuration
	DemoName          = "Long-Running Trading Simulator"
	DemoVersion       = "1.0.0"
	TargetDuration    = 10 * time.Minute // 10 minutes
	StatsInterval     = 3 * time.Minute
	RebalanceInterval = 10 * time.Second
	InitialCapital    = 10000.0 // $10,000 USD
)

func main() {
	fmt.Println(strings.Repeat("═", 80))
	fmt.Printf("🚀 %s v%s\n", DemoName, DemoVersion)
	fmt.Println(strings.Repeat("═", 80))
	fmt.Printf("⏱️  Target Duration: %s\n", TargetDuration)
	fmt.Printf("📊 Stats Interval: %s\n", StatsInterval)
	fmt.Printf("💼 Initial Capital: $%.2f\n", InitialCapital)
	fmt.Println(strings.Repeat("═", 80))

	// Initialize components
	fmt.Println("\n🔧 Initializing components...")

	stream := NewBinanceStream()
	fmt.Println("   ✅ BinanceStream created")

	simulator := NewTradingSimulator(InitialCapital)
	fmt.Println("   ✅ TradingSimulator created")

	statsCollector := NewStatsCollector()
	fmt.Println("   ✅ StatsCollector created")

	commandHandler := NewCommandHandler(stream, simulator, statsCollector)
	fmt.Println("   ✅ CommandHandler created")

	// Connect to Binance WebSocket
	fmt.Println("\n🌐 Connecting to Binance WebSocket...")
	if err := stream.Connect(); err != nil {
		fmt.Printf("❌ Failed to connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("   ✅ Connected to wss://stream.binance.com:9443")

	// Wait for initial prices
	fmt.Println("\n⏳ Waiting for initial price data...")
	for range 30 {
		if stream.GetCurrentBTC() > 0 && stream.GetCurrentETH() > 0 {
			fmt.Printf("   ✅ Initial prices received: BTC=$%.2f, ETH=$%.2f\n",
				stream.GetCurrentBTC(), stream.GetCurrentETH())
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if stream.GetCurrentBTC() == 0 || stream.GetCurrentETH() == 0 {
		fmt.Println("   ⚠️  Initial prices not received, continuing anyway...")
	}

	// Create Watcher config
	fmt.Println("\n🔍 Creating Hersh Watcher...")
	config := watch.DefaultWatcherConfig()
	config.DefaultTimeout = 5 * time.Minute
	config.RecoveryPolicy.MinConsecutiveFailures = 5
	config.RecoveryPolicy.MaxConsecutiveFailures = 10
	config.RecoveryPolicy.BaseRetryDelay = 10 * time.Second
	config.RecoveryPolicy.MaxRetryDelay = 5 * time.Minute
	config.RecoveryPolicy.LightweightRetryDelays = []time.Duration{
		30 * time.Second,
		1 * time.Minute,
		2 * time.Minute,
	}

	// Create context with 10-minute timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), TargetDuration)
	defer cancel()

	// Create Watcher with timeout context - it will auto-stop when context expires
	watcher := watch.NewWatcher(config)
	fmt.Println("   ✅ Watcher created with 10-minute timeout context")

	// Environment variables for managed function
	envVars := map[string]string{
		"DEMO_NAME":    DemoName,
		"DEMO_VERSION": DemoVersion,
	}

	// Register managed function with closure and envVars
	watcher.Manage(func(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
		return mainReducer(
			msg, ctx,
			stream,
			simulator,
			statsCollector,
			commandHandler,
		)
	}, "TradingSimulator", envVars).Cleanup(func(ctx watch.ManageContext) {
		cleanup(ctx, stream, simulator, statsCollector)
	})

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start Watcher
	fmt.Println("\n▶️  Starting main trading loop...")
	fmt.Println("   Type 'help' for available commands")
	fmt.Println("   Press Ctrl+C to stop")
	fmt.Println("   Watcher will auto-stop after 10 minutes")
	fmt.Println()

	if err := watcher.StartAndRun(); err != nil {
		fmt.Printf("❌ Initialization failed: %v\n", err)
		os.Exit(1)
	}
	// Wait for either context timeout or OS signal
	select {
	case <-ctx.Done():
		// Context timeout - watcher will auto-stop via parent context
		fmt.Println("\n\n⏰ Target duration reached (10 minutes)")
		fmt.Println("   Watcher auto-stopping gracefully...")
		// Brief pause to allow auto-stop to complete
		time.Sleep(200 * time.Millisecond)
	case <-sigChan:
		// User interrupt
		fmt.Println("\n\n🛑 Interrupt signal received...")
		watcher.StopAll()
	}

	// Start user input handler (only if stdin is available)
	go func() {
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			handleUserInput(watcher)
		}
	}()

	// Print logger summary
	fmt.Println("\n" + strings.Repeat("═", 80))
	fmt.Println("📋 EXECUTION LOGS")
	fmt.Println(strings.Repeat("═", 80))
	watcher.GetLogger().PrintSummary()

	fmt.Println("\n✅ Demo completed successfully")
	fmt.Println(strings.Repeat("═", 80))
}

// mainReducer is the main managed function for the Watcher
func mainReducer(
	msg *watch.Message,
	ctx watch.ManageContext,
	stream *BinanceStream,
	simulator *TradingSimulator,
	statsCollector *StatsCollector,
	commandHandler *CommandHandler,
) (watch.ControlSignal, error) {
	// WatchFlow: BTC price (real-time from WebSocket)
	btcHV := watch.WatchFlow[float64](0.0,
		flowValueStreamToFlowHandle(stream.GetBTCPriceStream()),
		"btc_price", ctx)
	// WatchFlow: ETH price (real-time from WebSocket)
	ethHV := watch.WatchFlow[float64](0.0,
		flowValueStreamToFlowHandle(stream.GetETHPriceStream()),
		"eth_price", ctx)

	// WatchTick: Stats ticker (1 minute interval)
	statsTick := watch.WatchTick("stats_ticker", StatsInterval, ctx)
	// WatchTick: Rebalance ticker (1 hour interval)
	rebalanceTick := watch.WatchTick("rebalance_ticker", RebalanceInterval, ctx)

	if btcHV.IsUpdatedValide() {
		simulator.UpdatePrice("BTC", btcHV.Value)
	}
	if ethHV.IsUpdatedValide() {
		simulator.UpdatePrice("ETH", ethHV.Value)
	}

	if statsTick.IsTriggered(ctx) {
		statsCollector.PrintStats(stream, simulator)
		watch.PrintWithLog(fmt.Sprintf("   (Stats tick #%d at %s)", statsTick.Value.TickCount, statsTick.Value.Time.Format("15:04:05")), ctx)
	}

	if rebalanceTick.IsTriggered(ctx) {
		watch.PrintWithLog(fmt.Sprintf("\n⏰ Hourly rebalance triggered (tick #%d at %s)...",
			rebalanceTick.Value.TickCount, rebalanceTick.Value.Time.Format("15:04:05")), ctx)
		trades := simulator.Rebalance()

		if len(trades) > 0 {
			watch.PrintWithLog(fmt.Sprintf("   Executed %d rebalancing trades", len(trades)), ctx)
			for _, t := range trades {
				watch.PrintWithLog(fmt.Sprintf("      %s %s: %.6f @ $%.2f",
					t.Action, t.Symbol, t.Amount, t.Price), ctx)
			}
		} else {
			watch.PrintWithLog("   No rebalancing needed", ctx)
		}
	}

	// Execute trading strategy (unless paused)
	if !simulator.IsPaused() {
		trades := simulator.ExecuteStrategy()

		if len(trades) > 0 {
			watch.PrintWithLog(fmt.Sprintf("\n💹 Strategy executed %d trades:", len(trades)), ctx)
			for _, t := range trades {
				watch.PrintWithLog(fmt.Sprintf("   %s %s %s: %.6f @ $%.2f (%s)",
					t.Time.Format("15:04:05"),
					t.Action, t.Symbol, t.Amount, t.Price, t.Reason), ctx)
			}

			portfolio := simulator.GetPortfolio()
			watch.PrintWithLog(fmt.Sprintf("   Portfolio Value: $%.2f (%.2f%%)",
				portfolio.CurrentValue, portfolio.ProfitLossPercent), ctx)
		}
	}

	// Handle user messages (commands)
	if msg != nil && msg.Content != "" {
		commandHandler.HandleCommand(msg.Content, ctx)
	}

	return watch.None(), nil
}

// cleanup is called when the Watcher stops
func cleanup(
	ctx watch.ManageContext,
	stream *BinanceStream,
	simulator *TradingSimulator,
	statsCollector *StatsCollector,
) {
	fmt.Println("\n🔧 Cleanup started...")

	// Close WebSocket
	fmt.Println("   Closing WebSocket...")
	stream.Close()

	// Print final statistics
	fmt.Println("\n📊 Final Statistics:")
	statsCollector.PrintDetailedStats(stream, simulator)

	fmt.Println("\n✅ Cleanup complete")
}

// handleUserInput reads user input and sends to Watcher
func handleUserInput(w *watch.Watcher) {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		input := scanner.Text()
		if input == "" {
			continue
		}

		// Send command to Watcher
		if err := w.SendMessage(input); err != nil {
			fmt.Printf("⚠️  Failed to send command: %v\n", err)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("⚠️  Input error: %v\n", err)
	}
}

// flowValueStreamToFlowHandle adapts a FlowValue[T] stream function to a wm.GetFlowHandleFunc[T].
// BinanceStream returns func(ctx) (<-chan FlowValue[T], error).
// WatchFlow expects func(flowCtx) (chan UpdateFunc[T], error).
func flowValueStreamToFlowHandle(
	getStream func(ctx context.Context) (<-chan shared.FlowValue[float64], error),
) wm.SetupUpdateFuncChan[float64] {
	return func(flowCtx wm.FlowContext) (chan wm.UpdateFunc[float64], error) {
		sourceCh, err := getStream(flowCtx)
		if err != nil {
			return nil, err
		}

		updateCh := make(chan wm.UpdateFunc[float64], 100)

		// Bridge: FlowValue → UpdateFunc
		go func() {
			defer close(updateCh)
			for fv := range sourceCh {
				val := fv // capture
				fn := func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
					if val.E != nil {
						return shared.WatchValue[float64]{Error: val.E}, false
					}
					if val.SkipSignal {
						return prev, true
					}
					return shared.WatchValue[float64]{Value: val.V}, false
				}
				updateCh <- fn
			}
		}()

		return updateCh, nil
	}
}
