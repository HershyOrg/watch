package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

// Trading strategy configuration
type TradingConfig struct {
	APIKey            string
	BuyThreshold      float64
	SellThreshold     float64
	MaxPosition       float64
	MonitoringEnabled bool
}

// Trading state
type TradingState struct {
	Position    float64
	TotalTrades int
	LastPrice   float64
}

// Global trading function - uses Watcher environment variables
func tradingFunc(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("[%s] Trading Cycle Started\n", time.Now().Format("15:04:05"))
	fmt.Println(strings.Repeat("=", 60))

	// Load configuration from environment variables
	apiKey, _ := ctx.GetValue("API_KEY").(string)
	buyThresholdStr, _ := ctx.GetValue("BUY_THRESHOLD").(string)
	sellThresholdStr, _ := ctx.GetValue("SELL_THRESHOLD").(string)
	maxPositionStr, _ := ctx.GetValue("MAX_POSITION").(string)
	buyThreshold, _ := strconv.ParseFloat(buyThresholdStr, 64)
	sellThreshold, _ := strconv.ParseFloat(sellThresholdStr, 64)
	maxPosition, _ := strconv.ParseFloat(maxPositionStr, 64)

	// Get monitoring state from context (can be toggled by messages)
	monitoringVal := ctx.GetValue("monitoring_enabled")
	if monitoringVal == nil {
		ctx.SetValue("monitoring_enabled", true)
		monitoringVal = true
	}
	monitoringEnabled := monitoringVal.(bool)

	// Initialize MarketClient with Memo (expensive operation, cached)
	client := watch.Memo(func() *MarketClient {
		fmt.Printf("\n[MEMO] Creating MarketClient (this happens only once)...")
		return NewMarketClient(apiKey)
	}, "marketClient", ctx)

	// Get trading state from context
	stateVal := ctx.GetValue("state")
	if stateVal == nil {
		// Initialize state on first run
		ctx.SetValue("state", &TradingState{
			Position:    0.0,
			TotalTrades: 0,
			LastPrice:   0.0,
		})
		stateVal = ctx.GetValue("state")
	}
	state := stateVal.(*TradingState)

	// Watch Bitcoin price - always outside conditional logic (generic version)
	priceHV := watch.WatchCall[float64](
		0.0, // Initial price value
		func(callCtx wm.CallContext) (func(runCtx wm.RunContext) wm.UpdateFunc[float64], error) {
			return func(runCtx wm.RunContext) wm.UpdateFunc[float64] {
				// 네트워크 요청은 여기서 미리 수행
				price, err := client.GetBitcoinPrice()
				if err != nil {
					return func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
						return shared.WatchValue[float64]{Error: err}, false
					}
				}
				return func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
					if prev.Value > 0 {
						if abs(price-prev.Value) > 100.0 {
							fmt.Printf("  [Watch] Price changed: $%.2f → $%.2f (Δ $%.2f)\n",
								prev.Value, price, price-prev.Value)
						}
					}
					return shared.WatchValue[float64]{Value: price}, false
				}
			}, nil
		},
		"btcPrice",
		500*time.Millisecond, // Poll every 500ms
		ctx,
	)

	// Process price data if monitoring is enabled
	if monitoringEnabled && priceHV.Value > 0 {
		currentPrice := priceHV.Value // Type-safe, no assertion needed

		fmt.Printf("\n📊 Current Bitcoin Price: $%.2f\n", currentPrice)
		fmt.Printf("💰 Current Position: $%.2f\n", state.Position)

		// Buy signal - use UpdateValue for safe state mutation
		if currentPrice < buyThreshold && state.Position < maxPosition {
			buyAmount := 1000.0
			fmt.Printf("\n🎯 BUY SIGNAL: Price $%.2f < Threshold $%.2f\n",
				currentPrice, buyThreshold)

			err := client.PlaceOrder("BUY", buyAmount)
			if err == nil {
				newState := ctx.UpdateValue("state", func(current any) any {
					s := current.(*TradingState)
					return &TradingState{
						Position:    s.Position + buyAmount,
						TotalTrades: s.TotalTrades + 1,
						LastPrice:   currentPrice,
					}
				}).(*TradingState)
				fmt.Printf("✅ Position updated: $%.2f\n", newState.Position)
				state = newState // Update local reference
			}
		}

		// Sell signal - use UpdateValue for safe state mutation
		if currentPrice > sellThreshold && state.Position > 0 {
			sellAmount := min(1000.0, state.Position)
			fmt.Printf("\n🎯 SELL SIGNAL: Price $%.2f > Threshold $%.2f\n",
				currentPrice, sellThreshold)

			err := client.PlaceOrder("SELL", sellAmount)
			if err == nil {
				newState := ctx.UpdateValue("state", func(current any) any {
					s := current.(*TradingState)
					return &TradingState{
						Position:    s.Position - sellAmount,
						TotalTrades: s.TotalTrades + 1,
						LastPrice:   currentPrice,
					}
				}).(*TradingState)
				fmt.Printf("✅ Position updated: $%.2f\n", newState.Position)
				state = newState // Update local reference
			}
		}

		// Get market depth periodically
		if state.TotalTrades > 0 && state.TotalTrades%3 == 0 {
			depth, err := client.GetMarketDepth("BTC-USD")
			if err == nil {
				fmt.Printf("\n📈 Market Depth: Bid=%.2f, Ask=%.2f, Volume=%.0f\n",
					depth["bid"], depth["ask"], depth["volume"])
			}
		}
	}

	// Handle user messages
	if msg != nil {
		fmt.Printf("\n💬 Message received: '%s'\n", msg.Content)

		switch msg.Content {
		case "status":
			fmt.Printf("\n📊 Trading Bot Status:\n")
			fmt.Printf("  - Last Price: $%.2f\n", state.LastPrice)
			fmt.Printf("  - Position: $%.2f\n", state.Position)
			fmt.Printf("  - Total Trades: %d\n", state.TotalTrades)
			fmt.Printf("  - Client: %s\n", client.GetStats())

		case "pause":
			ctx.SetValue("monitoring_enabled", false)
			fmt.Println("⏸️  Monitoring paused")

		case "resume":
			ctx.SetValue("monitoring_enabled", true)
			fmt.Println("▶️  Monitoring resumed")

		case "stop":
			fmt.Println("\n🛑 Stop signal received")
			fmt.Printf("Final Position: $%.2f\n", state.Position)
			fmt.Printf("Total Trades: %d\n", state.TotalTrades)
			return watch.Stop("user requested stop"), nil

		default:
			fmt.Printf("❓ Unknown command: %s\n", msg.Content)
		}
	}

	fmt.Println()
	fmt.Println(strings.Repeat("-", 60))
	return watch.None(), nil
}

// Global cleanup function
func cleanupFunc(ctx watch.ManageContext) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("[CLEANUP] Shutting down trading bot...")
	fmt.Println(strings.Repeat("=", 60))

	// Get client from Memo cache and close it
	client := watch.Memo(func() *MarketClient {
		return nil // Won't be called, value already cached
	}, "marketClient", ctx)

	if client != nil {
		client.Close()
	}

	// Print final statistics from context
	stateVal := ctx.GetValue("state")
	if stateVal != nil {
		state := stateVal.(*TradingState)
		fmt.Println("\n📊 Final Statistics:")
		fmt.Printf("  - Final Position: $%.2f\n", state.Position)
		fmt.Printf("  - Total Trades: %d\n", state.TotalTrades)
	}

	fmt.Println("\n✅ Cleanup complete")
}

func main() {
	fmt.Println("=== Hersh Trading Bot Demo ===")
	fmt.Println()
	fmt.Println("Polymarket + Bitcoin Price Monitor.")

	// Create watcher
	watcherConfig := watch.DefaultWatcherConfig()
	watcherConfig.DefaultTimeout = 10 * time.Second
	watcher := watch.NewWatcher(watcherConfig)

	// Create environment variables for managed function
	envVars := map[string]any{
		"API_KEY":        "demo-api-key-12345",
		"BUY_THRESHOLD":  "44500.00",
		"SELL_THRESHOLD": "45500.00",
		"MAX_POSITION":   "10000.00",
	}

	// Register global trading function with envVars and cleanup
	watcher.Manage(tradingFunc, "tradingBot", envVars).Cleanup(cleanupFunc)

	// Start watcher
	fmt.Println("Starting trading bot...")
	fmt.Printf("  [Before Start] State: %s\n", watcher.GetState())

	err := watcher.StartAndRun()
	if err != nil {
		panic(err)
	}

	// Check state immediately after Start
	fmt.Printf("  [After Start] State: %s\n", watcher.GetState())

	// Wait for initialization and check states during startup
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("  [+300ms] State: %s\n", watcher.GetState())

	time.Sleep(500 * time.Millisecond)
	fmt.Printf("  [+800ms] State: %s\n", watcher.GetState())

	fmt.Printf("\n✅ Trading bot started (State: %s)\n", watcher.GetState())
	fmt.Println("📡 WatcherAPI Server: http://localhost:8080")

	// Test WatcherAPI endpoints with state checks
	time.Sleep(500 * time.Millisecond)
	testWatcherAPI()

	// Simulate user interactions
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Simulating User Commands...")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("  [During Operation] State: %s\n", watcher.GetState())

	time.Sleep(2 * time.Second)
	fmt.Println("\n→ Sending 'status' command...")
	fmt.Printf("  [Before 'status'] State: %s\n", watcher.GetState())
	watcher.SendMessage("status")

	time.Sleep(2 * time.Second)
	fmt.Println("\n→ Sending 'pause' command...")
	fmt.Printf("  [Before 'pause'] State: %s\n", watcher.GetState())
	watcher.SendMessage("pause")

	time.Sleep(1 * time.Second)
	fmt.Println("\n→ Sending 'resume' command...")
	fmt.Printf("  [Before 'resume'] State: %s\n", watcher.GetState())
	watcher.SendMessage("resume")

	time.Sleep(2 * time.Second)
	fmt.Println("\n→ Sending 'stop' command...")
	fmt.Printf("  [Before 'stop'] State: %s\n", watcher.GetState())
	watcher.SendMessage("stop")

	// Wait for shutdown
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("  [+500ms after 'stop'] State: %s\n", watcher.GetState())

	time.Sleep(500 * time.Millisecond)
	fmt.Printf("  [+1000ms after 'stop'] State: %s\n", watcher.GetState())

	// Print logger summary
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Execution Summary")
	fmt.Println(strings.Repeat("=", 60))
	watcher.GetLogger().PrintSummary()

	// Stop watcher
	fmt.Printf("\n[Before watcher.StopAll()] State: %s\n", watcher.GetState())
	err = watcher.StopAll()
	if err != nil {
		fmt.Printf("Error stopping: %v\n", err)
	}
	fmt.Printf("[After watcher.StopAll()] State: %s\n", watcher.GetState())

	fmt.Println("\n=== Demo Complete ===")
}

// testWatcherAPI tests all WatcherAPI endpoints with pretty-printed request/response
func testWatcherAPI() {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Testing WatcherAPI Endpoints")
	fmt.Println(strings.Repeat("=", 60))

	baseURL := "http://localhost:8080"
	client := &http.Client{Timeout: 5 * time.Second}

	// Test 1: GET /watcher/status
	fmt.Println("\n[Test 1] GET /watcher/status")
	fmt.Println(strings.Repeat("-", 60))
	resp, err := client.Get(baseURL + "/watcher/status")
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		printResponse(resp)
	}

	// Test 2: GET /watcher/signals
	fmt.Println("\n[Test 2] GET /watcher/signals")
	fmt.Println(strings.Repeat("-", 60))
	resp, err = client.Get(baseURL + "/watcher/signals")
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		printResponse(resp)
	}

	// Test 3: GET /watcher/logs?type=effect&limit=5
	fmt.Println("\n[Test 3] GET /watcher/logs?type=effect&limit=5")
	fmt.Println(strings.Repeat("-", 60))
	resp, err = client.Get(baseURL + "/watcher/logs?type=effect&limit=5")
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		printResponse(resp)
	}

	// Test 4: GET /watcher/logs?type=context&limit=3
	fmt.Println("\n[Test 4] GET /watcher/logs?type=context&limit=3")
	fmt.Println(strings.Repeat("-", 60))
	resp, err = client.Get(baseURL + "/watcher/logs?type=context&limit=3")
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		printResponse(resp)
	}

	// Test 5: POST /watcher/message
	fmt.Println("\n[Test 5] POST /watcher/message")
	fmt.Println(strings.Repeat("-", 60))
	reqBody := map[string]string{"content": "status"}
	jsonData, _ := json.Marshal(reqBody)
	fmt.Printf("Request Body: %s\n", string(jsonData))
	resp, err = client.Post(
		baseURL+"/watcher/message",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		printResponse(resp)
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("WatcherAPI Tests Complete")
	fmt.Println(strings.Repeat("=", 60))
}

// printResponse pretty-prints HTTP response
func printResponse(resp *http.Response) {
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("❌ Error reading response: %v\n", err)
		return
	}

	fmt.Printf("Status: %s\n", resp.Status)

	// Try to pretty-print JSON
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, body, "", "  "); err == nil {
		fmt.Printf("Response:\n%s\n", prettyJSON.String())
	} else {
		fmt.Printf("Response: %s\n", string(body))
	}
}

// Helper functions
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
