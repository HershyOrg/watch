package main

import (
	"fmt"
	"strings"

	"github.com/HershyOrg/watch"
)

// CommandHandler handles user commands
type CommandHandler struct {
	bs    *BinanceStream
	ts    *TradingSimulator
	stats *StatsCollector
}

// NewCommandHandler creates a new command handler
func NewCommandHandler(bs *BinanceStream, ts *TradingSimulator, stats *StatsCollector) *CommandHandler {
	return &CommandHandler{
		bs:    bs,
		ts:    ts,
		stats: stats,
	}
}

// HandleCommand processes user commands
func (ch *CommandHandler) HandleCommand(cmd string, ctx watch.ManageContext) {
	cmd = strings.TrimSpace(strings.ToLower(cmd))

	switch cmd {
	case "help", "h", "?":
		ch.printHelp(ctx)

	case "status", "s":
		ch.stats.PrintStatus(ch.bs, ch.ts)

	case "stats", "st":
		ch.stats.PrintStats(ch.bs, ch.ts)

	case "detailed", "detail", "d":
		ch.stats.PrintDetailedStats(ch.bs, ch.ts)

	case "portfolio", "p":
		ch.stats.PrintPortfolio(ch.ts)

	case "trades", "t":
		ch.stats.PrintRecentTrades(ch.ts, 10)

	case "trades20", "t20":
		ch.stats.PrintRecentTrades(ch.ts, 20)

	case "trades50", "t50":
		ch.stats.PrintRecentTrades(ch.ts, 50)

	case "pause":
		ch.pauseTrading(ctx)

	case "resume":
		ch.resumeTrading(ctx)

	case "rebalance", "r":
		ch.rebalance(ctx)

	case "prices", "price":
		ch.printPrices(ctx)

	case "quit", "exit", "q":
		watch.PrintWithLog("\n⚠️  Use Ctrl+C to stop the demo gracefully", ctx)

	default:
		watch.PrintWithLog(fmt.Sprintf("\n❌ Unknown command: '%s'", cmd), ctx)
		watch.PrintWithLog("💡 Type 'help' to see available commands", ctx)
	}
}

// printHelp prints available commands
func (ch *CommandHandler) printHelp(ctx watch.ManageContext) {
	watch.PrintWithLog("\n"+strings.Repeat("═", 80), ctx)
	watch.PrintWithLog("📖 AVAILABLE COMMANDS", ctx)
	watch.PrintWithLog(strings.Repeat("═", 80), ctx)

	watch.PrintWithLog("\n📊 Statistics:", ctx)
	watch.PrintWithLog("   status, s          Show quick status summary", ctx)
	watch.PrintWithLog("   stats, st          Show 1-minute stats report", ctx)
	watch.PrintWithLog("   detailed, d        Show comprehensive detailed statistics", ctx)
	watch.PrintWithLog("   portfolio, p       Show detailed portfolio information", ctx)

	watch.PrintWithLog("\n📈 Trading:", ctx)
	watch.PrintWithLog("   trades, t          Show last 10 trades", ctx)
	watch.PrintWithLog("   trades20, t20      Show last 20 trades", ctx)
	watch.PrintWithLog("   trades50, t50      Show last 50 trades", ctx)
	watch.PrintWithLog("   pause              Pause trading (stop strategy execution)", ctx)
	watch.PrintWithLog("   resume             Resume trading", ctx)
	watch.PrintWithLog("   rebalance, r       Force portfolio rebalance", ctx)

	watch.PrintWithLog("\n💰 Market:", ctx)
	watch.PrintWithLog("   prices, price      Show current BTC/ETH prices", ctx)

	watch.PrintWithLog("\n❓ Other:", ctx)
	watch.PrintWithLog("   help, h, ?         Show this help message", ctx)
	watch.PrintWithLog("   quit, exit, q      Exit instructions (use Ctrl+C)", ctx)

	watch.PrintWithLog(strings.Repeat("═", 80), ctx)
}

// pauseTrading pauses trading strategy
func (ch *CommandHandler) pauseTrading(ctx watch.ManageContext) {
	if ch.ts.IsPaused() {
		watch.PrintWithLog("\n⚠️  Trading is already paused", ctx)
		return
	}

	ch.ts.Pause()
	watch.PrintWithLog("\n⏸️  Trading PAUSED", ctx)
	watch.PrintWithLog("   Strategy execution stopped", ctx)
	watch.PrintWithLog("   Price monitoring continues", ctx)
	watch.PrintWithLog("   Type 'resume' to restart trading", ctx)
}

// resumeTrading resumes trading strategy
func (ch *CommandHandler) resumeTrading(ctx watch.ManageContext) {
	if !ch.ts.IsPaused() {
		watch.PrintWithLog("\n⚠️  Trading is already active", ctx)
		return
	}

	ch.ts.Resume()
	watch.PrintWithLog("\n▶️  Trading RESUMED", ctx)
	watch.PrintWithLog("   Strategy execution restarted", ctx)
}

// rebalance forces portfolio rebalance
func (ch *CommandHandler) rebalance(ctx watch.ManageContext) {
	watch.PrintWithLog("\n🔄 Rebalancing portfolio...", ctx)

	trades := ch.ts.Rebalance()

	if len(trades) == 0 {
		watch.PrintWithLog("   No rebalancing needed (positions already balanced)", ctx)
		return
	}

	watch.PrintWithLog(fmt.Sprintf("   Executed %d rebalancing trades:", len(trades)), ctx)
	for _, t := range trades {
		watch.PrintWithLog(fmt.Sprintf("      %s %s: %.6f @ $%.2f = $%.2f",
			t.Action, t.Symbol, t.Amount, t.Price, t.USDValue), ctx)
	}

	portfolio := ch.ts.GetPortfolio()
	watch.PrintWithLog(fmt.Sprintf("   New Portfolio Value: $%.2f", portfolio.CurrentValue), ctx)
}

// printPrices prints current market prices
func (ch *CommandHandler) printPrices(ctx watch.ManageContext) {
	btcPrice := ch.bs.GetCurrentBTC()
	ethPrice := ch.bs.GetCurrentETH()
	streamStats := ch.bs.GetStats()

	watch.PrintWithLog("\n"+strings.Repeat("-", 50), ctx)
	watch.PrintWithLog("💰 Current Market Prices", ctx)
	watch.PrintWithLog(strings.Repeat("-", 50), ctx)

	if btcPrice == 0 || ethPrice == 0 {
		watch.PrintWithLog("   ⚠️  Prices not available yet", ctx)
		watch.PrintWithLog(fmt.Sprintf("   WebSocket Connected: %v", streamStats.Connected), ctx)
		watch.PrintWithLog(strings.Repeat("-", 50), ctx)
		return
	}

	watch.PrintWithLog(fmt.Sprintf("   🟠 BTC/USDT: $%.2f", btcPrice), ctx)
	watch.PrintWithLog(fmt.Sprintf("   🔵 ETH/USDT: $%.2f", ethPrice), ctx)
	watch.PrintWithLog(fmt.Sprintf("   📡 WebSocket: %v", streamStats.Connected), ctx)
	watch.PrintWithLog(fmt.Sprintf("   📨 Messages: %d", streamStats.MessagesReceived), ctx)

	watch.PrintWithLog(strings.Repeat("-", 50), ctx)
}
