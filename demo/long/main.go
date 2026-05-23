package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/demo/binance"
)

var a = "a"

func nonClosure() {
	defer print(a)
	print(a)
	return
}
func main() {
	print(a)
	//go 감지 제크용
	go nonClosure()

	config := watch.DefaultWatcherConfig()
	config.DefaultTimeout = 2 * time.Minute
	watcher := watch.NewWatcher(config)
	watcher.Manage(delcaredLogic, "TradingSimulator", map[string]any{
		"DEMO_NAME": "Long-Running Trading Simulator", "DEMO_VERSION": "1.0.0",
	}).Cleanup(cleanupReducer)

	{
		print(a)
	}
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

	if logger := watcher.Logger(); logger != nil {
		logger.PrintSummary()
	}
}

func delcaredLogic(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
	// WatchFlow: 실시간 가격 (Setup에서 WebSocket 생성+연결, FlowCtx 종료 시 자동 Close)
	btcHV := watch.WatchFlow(0.0, binance.BTCPriceFlow, "btc_price", ctx)
	ethHV := watch.WatchFlow(0.0, binance.ETHPriceFlow, "eth_price", ctx)

	// WatchTick
	statsTick := watch.WatchTick("stats_ticker", 3*time.Minute, ctx)
	// TradingSimulator (Memo: 1회 생성, 캐시)
	rebalanceTick := watch.WatchTick("rebalance_ticker", 10*time.Second, ctx)
	simulator := watch.Memo(func() *TradingSimulator {
		return NewTradingSimulator(10000.0)
	}, "sim", ctx)
	if msg == nil {
		msg = &watch.Message{}
	}
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

	// 전략: 신호 관측 → 정책 결정 → 효과 적용
	if !simulator.IsPaused() {
		var trades []Trade
		for _, sym := range []string{"BTC", "ETH"} {
			// MA5/MA15 크로스오버
			switch simulator.DetectCross(sym).Kind {
			case CrossGolden:
				if t := simulator.Buy(sym, 100, "golden_cross"); t != nil {
					trades = append(trades, *t)
				}
			case CrossDeath:
				if t := simulator.SellFraction(sym, 0.5, "death_cross"); t != nil {
					trades = append(trades, *t)
				}
			}

			// 2% 단기 변동
			switch simulator.DetectVolatility(sym, 0.02, 10).Kind {
			case VolSpike: // 급등 → 익절
				if t := simulator.SellFraction(sym, 0.3, "take_profit"); t != nil {
					trades = append(trades, *t)
				}
			case VolDip: // 급락 → 저가매수
				if t := simulator.Buy(sym, 50, "buy_dip"); t != nil {
					trades = append(trades, *t)
				}
			}
		}

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
	switch msg.Content {
	case "":
	case "status", "s":
		p := simulator.GetPortfolio()
		watch.PrintWithLog(fmt.Sprintf(
			"📊 BTC=$%.2f ETH=$%.2f | Portfolio: $%.2f (%.2f%%) | Trading: %v",
			btcHV.Value, ethHV.Value, p.CurrentValue, p.ProfitLossPercent, !simulator.IsPaused()), ctx)
	case "portfolio", "p":
		p := simulator.GetPortfolio()
		watch.PrintWithLog(fmt.Sprintf(
			"💼 $%.2f (%.2f%%) | BTC: %.6f ETH: %.6f | Cash: $%.2f",
			p.CurrentValue, p.ProfitLossPercent, p.BTCAmount, p.ETHAmount, p.CurrentUSD), ctx)
	case "trades", "t":
		trades := simulator.GetTrades()
		start := max(len(trades)-10, 0)
		for _, t := range trades[start:] {
			watch.PrintWithLog(fmt.Sprintf("   %s %s %s: %.6f @ $%.2f (%s)",
				t.Time.Format("15:04:05"), t.Action, t.Symbol, t.Amount, t.Price, t.Reason), ctx)
		}
	case "pause":
		simulator.Pause()
		watch.PrintWithLog("⏸️  Trading paused", ctx)
	case "resume":
		simulator.Resume()
		watch.PrintWithLog("▶️  Trading resumed", ctx)
	case "help", "h", "?":
		watch.PrintWithLog("Commands: status | portfolio | trades | pause | resume | help", ctx)
	default:
		watch.PrintWithLog("❌ Unknown command (type 'help')", ctx)
	}

	return watch.None(), nil
}

func cleanupReducer(ctx watch.ManageContext) {
	fmt.Println("\n🔧 Cleanup...")
	//* ctx사용은 가능
	ctx.GetValue("aa")

	// Stream cleanup: WatchFlow의 FlowContext 종료가 자동 처리
	fmt.Println("✅ Cleanup complete")
}
