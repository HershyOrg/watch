package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

func main() {

	watcher := watch.NewWatcher(watch.DefaultWatcherConfig())
	watcher.Manage(delcaredLogic, "TradingSimulator", map[string]any{
		"DEMO_NAME": "Long-Running Trading Simulator", "DEMO_VERSION": "1.0.0",
	}).Cleanup(cleanupReducer)

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

func delcaredLogic(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
	// WatchFlow: 실시간 가격 (Setup에서 WebSocket 생성+연결, FlowCtx 종료 시 자동 Close)
	btcHV := watch.WatchFlow(0.0, newBinancePriceFlow("BTC"), "btc_price", ctx)
	ethHV := watch.WatchFlow(0.0, newBinancePriceFlow("ETH"), "eth_price", ctx)

	// WatchTick
	statsTick := watch.WatchTick("stats_ticker", 3*time.Minute, ctx)
	rebalanceTick := watch.WatchTick("rebalance_ticker", 10*time.Second, ctx)

	// TradingSimulator (Memo: 1회 생성, 캐시)
	simulator := watch.Memo(func() *TradingSimulator {
		return NewTradingSimulator(10000.0)
	}, "sim", ctx)

	// 가격 반영
	if btcHV.IsUpdatedValid() {
		simulator.UpdatePrice("BTC", btcHV.Value)
	}
	if ethHV.IsUpdatedValid() {
		simulator.UpdatePrice("ETH", ethHV.Value)
	}

	// 주기적 리포트
	if statsTick.IsTriggered(ctx) {
		p := simulator.GetPortfolio()
		watch.PrintWithLog(fmt.Sprintf(
			"\n📊 [%s] BTC=$%.2f ETH=$%.2f | Portfolio: $%.2f (%.2f%%) | Trades: %d",
			time.Now().Format("15:04:05"),
			btcHV.Value, ethHV.Value,
			p.CurrentValue, p.ProfitLossPercent,
			len(simulator.GetTrades())), ctx)
	}

	// 리밸런스
	if rebalanceTick.IsTriggered(ctx) {
		trades := simulator.Rebalance()
		for _, t := range trades {
			watch.PrintWithLog(fmt.Sprintf("   ⏰ %s %s: %.6f @ $%.2f",
				t.Action, t.Symbol, t.Amount, t.Price), ctx)
		}
	}

	// 전략 실행
	if !simulator.IsPaused() {
		trades := simulator.ExecuteStrategy()
		if len(trades) > 0 {
			watch.PrintWithLog(fmt.Sprintf("\n💹 %d trades:", len(trades)), ctx)
			for _, t := range trades {
				watch.PrintWithLog(fmt.Sprintf("   %s %s %s: %.6f @ $%.2f (%s)",
					t.Time.Format("15:04:05"),
					t.Action, t.Symbol, t.Amount, t.Price, t.Reason), ctx)
			}
			p := simulator.GetPortfolio()
			watch.PrintWithLog(fmt.Sprintf("   Portfolio: $%.2f (%.2f%%)",
				p.CurrentValue, p.ProfitLossPercent), ctx)
		}
	}

	// 사용자 명령
	if msg != nil && msg.Content != "" {
		handleCommand(msg.Content, simulator, btcHV.Value, ethHV.Value, ctx)
	}

	return watch.None(), nil
}

// --- helpers ---

// newBinancePriceFlow returns a SetupUpdateFuncChan that fully owns its BinanceStream.
// Setup(1회): BinanceStream 생성 → Connect → 가격 채널 구독
// FlowCtx.Done(): WebSocket 자동 Close
func newBinancePriceFlow(symbol string) wm.SetupUpdateFuncChan[float64] {
	return func(flowCtx wm.FlowContext) (chan wm.UpdateFunc[float64], error) {
		stream := NewBinanceStream()
		if err := stream.Connect(); err != nil {
			return nil, fmt.Errorf("binance connect: %w", err)
		}

		var getStream func(ctx context.Context) (<-chan shared.FlowValue[float64], error)
		switch symbol {
		case "BTC":
			getStream = stream.GetBTCPriceStream()
		case "ETH":
			getStream = stream.GetETHPriceStream()
		default:
			stream.Close()
			return nil, fmt.Errorf("unknown symbol: %s", symbol)
		}

		sourceCh, err := getStream(flowCtx)
		if err != nil {
			stream.Close()
			return nil, err
		}

		updateCh := make(chan wm.UpdateFunc[float64], 100)
		go func() {
			defer close(updateCh)
			defer stream.Close()
			for {
				select {
				case fv, ok := <-sourceCh:
					if !ok {
						return
					}
					val := fv
					fn := func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
						if val.E != nil {
							return shared.WatchValue[float64]{Error: val.E}, false
						}
						if val.SkipSignal {
							return prev, true
						}
						return shared.WatchValue[float64]{Value: val.V}, false
					}
					select {
					case updateCh <- fn:
					case <-flowCtx.Done():
						return
					}
				case <-flowCtx.Done():
					return
				}
			}
		}()

		return updateCh, nil
	}
}

func handleCommand(cmd string, sim *TradingSimulator, btcPrice, ethPrice float64, ctx watch.ManageContext) {
	cmd = strings.TrimSpace(strings.ToLower(cmd))
	switch cmd {
	case "status", "s":
		p := sim.GetPortfolio()
		watch.PrintWithLog(fmt.Sprintf(
			"📊 BTC=$%.2f ETH=$%.2f | Portfolio: $%.2f (%.2f%%) | Trading: %v",
			btcPrice, ethPrice, p.CurrentValue, p.ProfitLossPercent, !sim.IsPaused()), ctx)
	case "portfolio", "p":
		p := sim.GetPortfolio()
		watch.PrintWithLog(fmt.Sprintf(
			"💼 $%.2f (%.2f%%) | BTC: %.6f ETH: %.6f | Cash: $%.2f",
			p.CurrentValue, p.ProfitLossPercent, p.BTCAmount, p.ETHAmount, p.CurrentUSD), ctx)
	case "trades", "t":
		trades := sim.GetTrades()
		start := len(trades) - 10
		if start < 0 {
			start = 0
		}
		for _, t := range trades[start:] {
			watch.PrintWithLog(fmt.Sprintf("   %s %s %s: %.6f @ $%.2f (%s)",
				t.Time.Format("15:04:05"), t.Action, t.Symbol, t.Amount, t.Price, t.Reason), ctx)
		}
	case "pause":
		sim.Pause()
		watch.PrintWithLog("⏸️  Trading paused", ctx)
	case "resume":
		sim.Resume()
		watch.PrintWithLog("▶️  Trading resumed", ctx)
	case "help", "h", "?":
		watch.PrintWithLog("Commands: status | portfolio | trades | pause | resume | help", ctx)
	default:
		watch.PrintWithLog(fmt.Sprintf("❌ Unknown: '%s' (type 'help')", cmd), ctx)
	}
}

func cleanupReducer(ctx watch.ManageContext) {
	fmt.Println("\n🔧 Cleanup...")
	// Stream cleanup: WatchFlow의 FlowContext 종료가 자동 처리
	fmt.Println("✅ Cleanup complete")
}
