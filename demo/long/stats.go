package main

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// StatsCollector holds runtime statistics for the demo
type StatsCollector struct {
	startTime time.Time
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector() *StatsCollector {
	return &StatsCollector{
		startTime: time.Now(),
	}
}

// PrintStats prints 1-minute automatic statistics
func (sc *StatsCollector) PrintStats(bs *BinanceStream, ts *TradingSimulator) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("⏰ Stats Report - %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(strings.Repeat("=", 80))

	// Uptime
	uptime := time.Since(sc.startTime)
	fmt.Printf("📊 Uptime: %s\n", formatDuration(uptime))

	// WebSocket stats
	streamStats := bs.GetStats()
	fmt.Printf("\n🌐 WebSocket Status:\n")
	fmt.Printf("   Connected: %v\n", streamStats.Connected)
	fmt.Printf("   Messages Received: %d\n", streamStats.MessagesReceived)
	fmt.Printf("   Reconnects: %d\n", streamStats.Reconnects)
	fmt.Printf("   Errors: %d\n", streamStats.Errors)
	fmt.Printf("   Last Update: %s ago\n", time.Since(streamStats.LastUpdate).Round(time.Second))

	// Current prices
	btcPrice := bs.GetCurrentBTC()
	ethPrice := bs.GetCurrentETH()
	fmt.Printf("\n💰 Current Prices:\n")
	fmt.Printf("   BTC: $%.2f\n", btcPrice)
	fmt.Printf("   ETH: $%.2f\n", ethPrice)

	// Portfolio stats
	portfolio := ts.GetPortfolio()
	fmt.Printf("\n💼 Portfolio:\n")
	fmt.Printf("   Total Value: $%.2f (%.2f%%)\n",
		portfolio.CurrentValue, portfolio.ProfitLossPercent)
	fmt.Printf("   Cash: $%.2f\n", portfolio.CurrentUSD)
	fmt.Printf("   BTC: %.6f ($%.2f)\n",
		portfolio.BTCAmount, portfolio.BTCAmount*portfolio.CurrentBTC)
	fmt.Printf("   ETH: %.6f ($%.2f)\n",
		portfolio.ETHAmount, portfolio.ETHAmount*portfolio.CurrentETH)

	// Trading stats
	trades := ts.GetTrades()
	fmt.Printf("\n📈 Trading:\n")
	fmt.Printf("   Total Trades: %d\n", len(trades))
	fmt.Printf("   Trading: %v\n", !ts.IsPaused())

	// System stats
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("\n🖥️  System:\n")
	fmt.Printf("   Memory: %s\n", formatBytes(mem.Alloc))
	fmt.Printf("   Goroutines: %d\n", runtime.NumGoroutine())

	fmt.Println(strings.Repeat("=", 80))
}

// PrintDetailedStats prints comprehensive statistics on demand
func (sc *StatsCollector) PrintDetailedStats(bs *BinanceStream, ts *TradingSimulator) {
	fmt.Println("\n" + strings.Repeat("━", 80))
	fmt.Printf("📋 DETAILED STATISTICS - %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(strings.Repeat("━", 80))

	// Runtime information
	uptime := time.Since(sc.startTime)
	fmt.Printf("\n⏱️  Runtime Information:\n")
	fmt.Printf("   Started: %s\n", sc.startTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("   Uptime: %s\n", formatDuration(uptime))
	fmt.Printf("   Current Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))

	// WebSocket detailed stats
	streamStats := bs.GetStats()
	fmt.Printf("\n🌐 WebSocket Detailed Stats:\n")
	fmt.Printf("   Connection Status: %v\n", streamStats.Connected)
	fmt.Printf("   Messages Received: %d\n", streamStats.MessagesReceived)
	if uptime.Seconds() > 0 {
		msgPerSec := float64(streamStats.MessagesReceived) / uptime.Seconds()
		fmt.Printf("   Messages/Second: %.2f\n", msgPerSec)
	}
	fmt.Printf("   Reconnection Count: %d\n", streamStats.Reconnects)
	fmt.Printf("   Error Count: %d\n", streamStats.Errors)
	fmt.Printf("   Last Update: %s\n", streamStats.LastUpdate.Format("15:04:05"))

	// Portfolio detailed stats
	portfolio := ts.GetPortfolio()
	fmt.Printf("\n💼 Portfolio Detailed Stats:\n")
	fmt.Printf("   Initial Capital: $%.2f\n", portfolio.InitialUSD)
	fmt.Printf("   Current Value: $%.2f\n", portfolio.CurrentValue)
	fmt.Printf("   Profit/Loss: $%.2f (%.2f%%)\n",
		portfolio.ProfitLoss, portfolio.ProfitLossPercent)
	fmt.Printf("\n   Cash Position: $%.2f (%.1f%%)\n",
		portfolio.CurrentUSD, (portfolio.CurrentUSD/portfolio.CurrentValue)*100)

	btcValue := portfolio.BTCAmount * portfolio.CurrentBTC
	ethValue := portfolio.ETHAmount * portfolio.CurrentETH

	fmt.Printf("\n   BTC Holdings:\n")
	fmt.Printf("      Amount: %.6f BTC\n", portfolio.BTCAmount)
	fmt.Printf("      Avg Buy Price: $%.2f\n", portfolio.BTCAvgPrice)
	fmt.Printf("      Current Price: $%.2f\n", portfolio.CurrentBTC)
	fmt.Printf("      Current Value: $%.2f (%.1f%%)\n",
		btcValue, (btcValue/portfolio.CurrentValue)*100)
	if portfolio.BTCAmount > 0 {
		btcPnL := ((portfolio.CurrentBTC - portfolio.BTCAvgPrice) / portfolio.BTCAvgPrice) * 100
		fmt.Printf("      P&L: %.2f%%\n", btcPnL)
	}

	fmt.Printf("\n   ETH Holdings:\n")
	fmt.Printf("      Amount: %.6f ETH\n", portfolio.ETHAmount)
	fmt.Printf("      Avg Buy Price: $%.2f\n", portfolio.ETHAvgPrice)
	fmt.Printf("      Current Price: $%.2f\n", portfolio.CurrentETH)
	fmt.Printf("      Current Value: $%.2f (%.1f%%)\n",
		ethValue, (ethValue/portfolio.CurrentValue)*100)
	if portfolio.ETHAmount > 0 {
		ethPnL := ((portfolio.CurrentETH - portfolio.ETHAvgPrice) / portfolio.ETHAvgPrice) * 100
		fmt.Printf("      P&L: %.2f%%\n", ethPnL)
	}

	// Trading detailed stats
	trades := ts.GetTrades()
	fmt.Printf("\n📈 Trading Detailed Stats:\n")
	fmt.Printf("   Total Trades: %d\n", len(trades))

	if len(trades) > 0 {
		buyCount := 0
		sellCount := 0
		btcTrades := 0
		ethTrades := 0

		for _, t := range trades {
			if t.Action == "BUY" {
				buyCount++
			} else {
				sellCount++
			}
			if t.Symbol == "BTC" {
				btcTrades++
			} else {
				ethTrades++
			}
		}

		fmt.Printf("   Buy Orders: %d\n", buyCount)
		fmt.Printf("   Sell Orders: %d\n", sellCount)
		fmt.Printf("   BTC Trades: %d\n", btcTrades)
		fmt.Printf("   ETH Trades: %d\n", ethTrades)

		if uptime.Hours() > 0 {
			tradesPerHour := float64(len(trades)) / uptime.Hours()
			fmt.Printf("   Trades/Hour: %.2f\n", tradesPerHour)
		}
	}
	fmt.Printf("   Trading Status: %v\n", !ts.IsPaused())

	// System detailed stats
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("\n🖥️  System Detailed Stats:\n")
	fmt.Printf("   Memory Allocated: %s\n", formatBytes(mem.Alloc))
	fmt.Printf("   Total Allocated: %s\n", formatBytes(mem.TotalAlloc))
	fmt.Printf("   System Memory: %s\n", formatBytes(mem.Sys))
	fmt.Printf("   GC Runs: %d\n", mem.NumGC)
	fmt.Printf("   Goroutines: %d\n", runtime.NumGoroutine())

	fmt.Println(strings.Repeat("━", 80))
}

// PrintStatus prints current status summary
func (sc *StatsCollector) PrintStatus(bs *BinanceStream, ts *TradingSimulator) {
	fmt.Println("\n" + strings.Repeat("-", 60))
	fmt.Printf("📌 Status - %s\n", time.Now().Format("15:04:05"))
	fmt.Println(strings.Repeat("-", 60))

	uptime := time.Since(sc.startTime)
	streamStats := bs.GetStats()
	portfolio := ts.GetPortfolio()
	trades := ts.GetTrades()

	fmt.Printf("⏱️  Uptime: %s\n", formatDuration(uptime))
	fmt.Printf("🌐 WebSocket: %v | Messages: %d\n",
		streamStats.Connected, streamStats.MessagesReceived)
	fmt.Printf("💰 Prices: BTC $%.2f | ETH $%.2f\n",
		bs.GetCurrentBTC(), bs.GetCurrentETH())
	fmt.Printf("💼 Portfolio: $%.2f (%.2f%%) | Trades: %d\n",
		portfolio.CurrentValue, portfolio.ProfitLossPercent, len(trades))
	fmt.Printf("📈 Trading: %v\n", !ts.IsPaused())

	fmt.Println(strings.Repeat("-", 60))
}

// PrintPortfolio prints detailed portfolio information
func (sc *StatsCollector) PrintPortfolio(ts *TradingSimulator) {
	portfolio := ts.GetPortfolio()

	fmt.Println("\n" + strings.Repeat("─", 70))
	fmt.Printf("💼 PORTFOLIO DETAILS - %s\n", time.Now().Format("15:04:05"))
	fmt.Println(strings.Repeat("─", 70))

	btcValue := portfolio.BTCAmount * portfolio.CurrentBTC
	ethValue := portfolio.ETHAmount * portfolio.CurrentETH

	fmt.Printf("\n📊 Overview:\n")
	fmt.Printf("   Initial Capital:  $%12.2f\n", portfolio.InitialUSD)
	fmt.Printf("   Current Value:    $%12.2f\n", portfolio.CurrentValue)
	fmt.Printf("   Profit/Loss:      $%12.2f (%+.2f%%)\n",
		portfolio.ProfitLoss, portfolio.ProfitLossPercent)

	fmt.Printf("\n💵 Cash:\n")
	fmt.Printf("   Amount:           $%12.2f (%.1f%%)\n",
		portfolio.CurrentUSD, (portfolio.CurrentUSD/portfolio.CurrentValue)*100)

	fmt.Printf("\n🟠 Bitcoin (BTC):\n")
	fmt.Printf("   Holdings:         %12.6f BTC\n", portfolio.BTCAmount)
	fmt.Printf("   Avg Buy Price:    $%12.2f\n", portfolio.BTCAvgPrice)
	fmt.Printf("   Current Price:    $%12.2f\n", portfolio.CurrentBTC)
	fmt.Printf("   Position Value:   $%12.2f (%.1f%%)\n",
		btcValue, (btcValue/portfolio.CurrentValue)*100)
	if portfolio.BTCAmount > 0 {
		btcPnL := ((portfolio.CurrentBTC - portfolio.BTCAvgPrice) / portfolio.BTCAvgPrice) * 100
		fmt.Printf("   Unrealized P&L:   %+12.2f%%\n", btcPnL)
	}

	fmt.Printf("\n🔵 Ethereum (ETH):\n")
	fmt.Printf("   Holdings:         %12.6f ETH\n", portfolio.ETHAmount)
	fmt.Printf("   Avg Buy Price:    $%12.2f\n", portfolio.ETHAvgPrice)
	fmt.Printf("   Current Price:    $%12.2f\n", portfolio.CurrentETH)
	fmt.Printf("   Position Value:   $%12.2f (%.1f%%)\n",
		ethValue, (ethValue/portfolio.CurrentValue)*100)
	if portfolio.ETHAmount > 0 {
		ethPnL := ((portfolio.CurrentETH - portfolio.ETHAvgPrice) / portfolio.ETHAvgPrice) * 100
		fmt.Printf("   Unrealized P&L:   %+12.2f%%\n", ethPnL)
	}

	fmt.Println(strings.Repeat("─", 70))
}

// PrintRecentTrades prints recent trade history
func (sc *StatsCollector) PrintRecentTrades(ts *TradingSimulator, count int) {
	trades := ts.GetTrades()

	fmt.Println("\n" + strings.Repeat("─", 100))
	fmt.Printf("📊 RECENT TRADES (Last %d) - %s\n", count, time.Now().Format("15:04:05"))
	fmt.Println(strings.Repeat("─", 100))

	if len(trades) == 0 {
		fmt.Println("   No trades executed yet.")
		fmt.Println(strings.Repeat("─", 100))
		return
	}

	// Get last N trades
	start := 0
	if len(trades) > count {
		start = len(trades) - count
	}
	recentTrades := trades[start:]

	// Print header
	fmt.Printf("%-19s %-6s %-4s %12s %12s %12s %-15s\n",
		"Time", "Symbol", "Side", "Price", "Amount", "Value", "Reason")
	fmt.Println(strings.Repeat("-", 100))

	// Print trades
	for _, t := range recentTrades {
		fmt.Printf("%-19s %-6s %-4s $%11.2f %12.6f $%11.2f %-15s\n",
			t.Time.Format("15:04:05"),
			t.Symbol,
			t.Action,
			t.Price,
			t.Amount,
			t.USDValue,
			t.Reason)
	}

	fmt.Println(strings.Repeat("─", 100))
}

// formatDuration formats a duration into human-readable string
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

// formatBytes formats bytes into human-readable string
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
