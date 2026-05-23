package main

import (
	"sync"
	"sync/atomic"
	"time"
)

// TradingSimulator simulates dry-run cryptocurrency trading
type TradingSimulator struct {
	portfolio    Portfolio
	priceHistory PriceHistory
	trades       []Trade
	paused       atomic.Bool

	// Strategy state
	lastCrossover struct {
		btc string // "up", "down", or ""
		eth string
	}

	mu sync.RWMutex
}

// Portfolio represents the current holdings
type Portfolio struct {
	InitialUSD  float64
	CurrentUSD  float64
	BTCAmount   float64
	ETHAmount   float64
	BTCAvgPrice float64
	ETHAvgPrice float64
	CurrentBTC  float64 // Current BTC price
	CurrentETH  float64 // Current ETH price
}

// PriceHistory stores recent price points for MA calculation
type PriceHistory struct {
	btcPrices []PricePoint
	ethPrices []PricePoint
	maxPoints int
}

// PricePoint represents a price at a specific time
type PricePoint struct {
	Time  time.Time
	Price float64
}

// Trade represents a single trade execution (dry-run)
type Trade struct {
	Time           time.Time
	Symbol         string // "BTC" or "ETH"
	Action         string // "BUY" or "SELL"
	Price          float64
	Amount         float64
	USDValue       float64
	Reason         string
	PortfolioValue float64
}

// NewTradingSimulator creates a new trading simulator
func NewTradingSimulator(initialUSD float64) *TradingSimulator {
	return &TradingSimulator{
		portfolio: Portfolio{
			InitialUSD: initialUSD,
			CurrentUSD: initialUSD,
			BTCAmount:  0,
			ETHAmount:  0,
		},
		priceHistory: PriceHistory{
			btcPrices: make([]PricePoint, 0, 100),
			ethPrices: make([]PricePoint, 0, 100),
			maxPoints: 100,
		},
		trades: make([]Trade, 0, 1000),
	}
}

// UpdatePrice updates the price history
func (ts *TradingSimulator) UpdatePrice(symbol string, price float64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	point := PricePoint{
		Time:  time.Now(),
		Price: price,
	}

	switch symbol {
	case "BTC":
		ts.priceHistory.btcPrices = append(ts.priceHistory.btcPrices, point)
		if len(ts.priceHistory.btcPrices) > ts.priceHistory.maxPoints {
			ts.priceHistory.btcPrices = ts.priceHistory.btcPrices[1:]
		}
		ts.portfolio.CurrentBTC = price

	case "ETH":
		ts.priceHistory.ethPrices = append(ts.priceHistory.ethPrices, point)
		if len(ts.priceHistory.ethPrices) > ts.priceHistory.maxPoints {
			ts.priceHistory.ethPrices = ts.priceHistory.ethPrices[1:]
		}
		ts.portfolio.CurrentETH = price
	}
}

// CrossKind represents a moving-average crossover transition.
type CrossKind int

const (
	CrossNone CrossKind = iota
	CrossGolden
	CrossDeath
)

// CrossSignal is the observation returned by DetectCross.
type CrossSignal struct {
	Kind CrossKind
	MA5  float64
	MA15 float64
}

// VolKind represents a short-window volatility event.
type VolKind int

const (
	VolNone VolKind = iota
	VolSpike
	VolDip
)

// VolSignal is the observation returned by DetectVolatility.
type VolSignal struct {
	Kind          VolKind
	ChangePercent float64
}

// DetectCross observes MA5 vs MA15 and reports a transition relative to the
// previously observed direction. The internal direction is advanced as a side
// effect so each transition fires exactly once.
func (ts *TradingSimulator) DetectCross(symbol string) CrossSignal {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var prices []PricePoint
	var lastPtr *string
	switch symbol {
	case "BTC":
		prices = ts.priceHistory.btcPrices
		lastPtr = &ts.lastCrossover.btc
	case "ETH":
		prices = ts.priceHistory.ethPrices
		lastPtr = &ts.lastCrossover.eth
	default:
		return CrossSignal{}
	}

	if len(prices) < 15 {
		return CrossSignal{}
	}

	ma5 := ts.calculateMA(symbol, 5)
	ma15 := ts.calculateMA(symbol, 15)

	current := ""
	if ma5 > ma15 {
		current = "up"
	} else if ma5 < ma15 {
		current = "down"
	}

	kind := CrossNone
	if *lastPtr == "down" && current == "up" {
		kind = CrossGolden
	} else if *lastPtr == "up" && current == "down" {
		kind = CrossDeath
	}
	*lastPtr = current

	return CrossSignal{Kind: kind, MA5: ma5, MA15: ma15}
}

// DetectVolatility observes price change over the last `lookback` points and
// reports Spike/Dip if |change| exceeds threshold.
func (ts *TradingSimulator) DetectVolatility(symbol string, threshold float64, lookback int) VolSignal {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	var prices []PricePoint
	switch symbol {
	case "BTC":
		prices = ts.priceHistory.btcPrices
	case "ETH":
		prices = ts.priceHistory.ethPrices
	default:
		return VolSignal{}
	}

	if len(prices) < lookback {
		return VolSignal{}
	}

	oldPrice := prices[len(prices)-lookback].Price
	currentPrice := prices[len(prices)-1].Price
	change := (currentPrice - oldPrice) / oldPrice

	switch {
	case change > threshold:
		return VolSignal{Kind: VolSpike, ChangePercent: change}
	case change < -threshold:
		return VolSignal{Kind: VolDip, ChangePercent: change}
	default:
		return VolSignal{Kind: VolNone, ChangePercent: change}
	}
}

// Buy executes a USD-denominated buy. Returns nil if funds or price unavailable.
func (ts *TradingSimulator) Buy(symbol string, usdAmount float64, reason string) *Trade {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.executeBuy(symbol, usdAmount, reason)
}

// Sell executes a sell for a specific coin amount.
func (ts *TradingSimulator) Sell(symbol string, amount float64, reason string) *Trade {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.executeSell(symbol, amount, reason)
}

// SellFraction sells `frac` of the current holding for `symbol`.
// Returns nil if holding is too small to be meaningful.
func (ts *TradingSimulator) SellFraction(symbol string, frac float64, reason string) *Trade {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var holding, minHolding float64
	switch symbol {
	case "BTC":
		holding = ts.portfolio.BTCAmount
		minHolding = 0.001
	case "ETH":
		holding = ts.portfolio.ETHAmount
		minHolding = 0.01
	default:
		return nil
	}
	if holding < minHolding {
		return nil
	}
	return ts.executeSell(symbol, holding*frac, reason)
}

// calculateMA calculates moving average (must be called with lock held)
func (ts *TradingSimulator) calculateMA(symbol string, period int) float64 {
	var prices []PricePoint
	switch symbol {
	case "BTC":
		prices = ts.priceHistory.btcPrices
	case "ETH":
		prices = ts.priceHistory.ethPrices
	default:
		return 0
	}

	if len(prices) < period {
		return 0
	}

	sum := 0.0
	for i := len(prices) - period; i < len(prices); i++ {
		sum += prices[i].Price
	}

	return sum / float64(period)
}

// executeBuy executes a buy order (dry-run, must be called with lock held)
func (ts *TradingSimulator) executeBuy(symbol string, usdAmount float64, reason string) *Trade {
	if ts.portfolio.CurrentUSD < usdAmount {
		return nil
	}

	var price float64
	switch symbol {
	case "BTC":
		price = ts.portfolio.CurrentBTC
	case "ETH":
		price = ts.portfolio.CurrentETH
	default:
		return nil
	}

	if price == 0 {
		return nil
	}

	amount := usdAmount / price

	// Update portfolio
	ts.portfolio.CurrentUSD -= usdAmount

	switch symbol {
	case "BTC":
		oldTotal := ts.portfolio.BTCAmount * ts.portfolio.BTCAvgPrice
		newTotal := oldTotal + usdAmount
		ts.portfolio.BTCAmount += amount
		ts.portfolio.BTCAvgPrice = newTotal / ts.portfolio.BTCAmount

	case "ETH":
		oldTotal := ts.portfolio.ETHAmount * ts.portfolio.ETHAvgPrice
		newTotal := oldTotal + usdAmount
		ts.portfolio.ETHAmount += amount
		ts.portfolio.ETHAvgPrice = newTotal / ts.portfolio.ETHAmount
	}

	// Record trade
	trade := Trade{
		Time:           time.Now(),
		Symbol:         symbol,
		Action:         "BUY",
		Price:          price,
		Amount:         amount,
		USDValue:       usdAmount,
		Reason:         reason,
		PortfolioValue: ts.getPortfolioValue(),
	}

	ts.trades = append(ts.trades, trade)

	return &trade
}

// executeSell executes a sell order (dry-run, must be called with lock held)
func (ts *TradingSimulator) executeSell(symbol string, amount float64, reason string) *Trade {
	var price float64
	var currentAmount float64

	switch symbol {
	case "BTC":
		price = ts.portfolio.CurrentBTC
		currentAmount = ts.portfolio.BTCAmount
	case "ETH":
		price = ts.portfolio.CurrentETH
		currentAmount = ts.portfolio.ETHAmount
	default:
		return nil
	}

	if price == 0 || currentAmount < amount {
		return nil
	}

	usdValue := amount * price

	// Update portfolio
	ts.portfolio.CurrentUSD += usdValue

	switch symbol {
	case "BTC":
		ts.portfolio.BTCAmount -= amount
		if ts.portfolio.BTCAmount < 0.000001 {
			ts.portfolio.BTCAmount = 0
			ts.portfolio.BTCAvgPrice = 0
		}

	case "ETH":
		ts.portfolio.ETHAmount -= amount
		if ts.portfolio.ETHAmount < 0.000001 {
			ts.portfolio.ETHAmount = 0
			ts.portfolio.ETHAvgPrice = 0
		}
	}

	// Record trade
	trade := Trade{
		Time:           time.Now(),
		Symbol:         symbol,
		Action:         "SELL",
		Price:          price,
		Amount:         amount,
		USDValue:       usdValue,
		Reason:         reason,
		PortfolioValue: ts.getPortfolioValue(),
	}

	ts.trades = append(ts.trades, trade)

	return &trade
}

// Rebalance rebalances the portfolio (50:50 BTC:ETH, 20% cash)
func (ts *TradingSimulator) Rebalance() []Trade {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.portfolio.CurrentBTC == 0 || ts.portfolio.CurrentETH == 0 {
		return nil
	}

	var rebalanceTrades []Trade

	// Calculate current values
	totalValue := ts.getPortfolioValue()
	targetCrypto := totalValue * 0.8
	targetBTC := targetCrypto * 0.5
	targetETH := targetCrypto * 0.5

	currentBTCValue := ts.portfolio.BTCAmount * ts.portfolio.CurrentBTC
	currentETHValue := ts.portfolio.ETHAmount * ts.portfolio.CurrentETH

	// Rebalance BTC
	btcDiff := targetBTC - currentBTCValue
	if abs(btcDiff) > 50 { // Rebalance if difference > $50
		if btcDiff > 0 {
			// Buy BTC
			trade := ts.executeBuy("BTC", btcDiff, "rebalance")
			if trade != nil {
				rebalanceTrades = append(rebalanceTrades, *trade)
			}
		} else {
			// Sell BTC
			sellAmount := abs(btcDiff) / ts.portfolio.CurrentBTC
			trade := ts.executeSell("BTC", sellAmount, "rebalance")
			if trade != nil {
				rebalanceTrades = append(rebalanceTrades, *trade)
			}
		}
	}

	// Rebalance ETH
	ethDiff := targetETH - currentETHValue
	if abs(ethDiff) > 50 {
		if ethDiff > 0 {
			// Buy ETH
			trade := ts.executeBuy("ETH", ethDiff, "rebalance")
			if trade != nil {
				rebalanceTrades = append(rebalanceTrades, *trade)
			}
		} else {
			// Sell ETH
			sellAmount := abs(ethDiff) / ts.portfolio.CurrentETH
			trade := ts.executeSell("ETH", sellAmount, "rebalance")
			if trade != nil {
				rebalanceTrades = append(rebalanceTrades, *trade)
			}
		}
	}

	return rebalanceTrades
}

// getPortfolioValue calculates total portfolio value in USD (must be called with lock held)
func (ts *TradingSimulator) getPortfolioValue() float64 {
	btcValue := ts.portfolio.BTCAmount * ts.portfolio.CurrentBTC
	ethValue := ts.portfolio.ETHAmount * ts.portfolio.CurrentETH
	return ts.portfolio.CurrentUSD + btcValue + ethValue
}

// GetPortfolio returns a copy of the current portfolio with calculated values
func (ts *TradingSimulator) GetPortfolio() PortfolioSnapshot {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	totalValue := ts.getPortfolioValue()
	profitLoss := totalValue - ts.portfolio.InitialUSD
	profitLossPercent := (profitLoss / ts.portfolio.InitialUSD) * 100

	return PortfolioSnapshot{
		InitialUSD:        ts.portfolio.InitialUSD,
		CurrentUSD:        ts.portfolio.CurrentUSD,
		BTCAmount:         ts.portfolio.BTCAmount,
		ETHAmount:         ts.portfolio.ETHAmount,
		BTCAvgPrice:       ts.portfolio.BTCAvgPrice,
		ETHAvgPrice:       ts.portfolio.ETHAvgPrice,
		CurrentBTC:        ts.portfolio.CurrentBTC,
		CurrentETH:        ts.portfolio.CurrentETH,
		CurrentValue:      totalValue,
		ProfitLoss:        profitLoss,
		ProfitLossPercent: profitLossPercent,
	}
}

// PortfolioSnapshot is a read-only snapshot of the portfolio
type PortfolioSnapshot struct {
	InitialUSD        float64
	CurrentUSD        float64
	BTCAmount         float64
	ETHAmount         float64
	BTCAvgPrice       float64
	ETHAvgPrice       float64
	CurrentBTC        float64
	CurrentETH        float64
	CurrentValue      float64
	ProfitLoss        float64
	ProfitLossPercent float64
}

// GetTrades returns a copy of all trades
func (ts *TradingSimulator) GetTrades() []Trade {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	trades := make([]Trade, len(ts.trades))
	copy(trades, ts.trades)
	return trades
}

// Pause pauses trading
func (ts *TradingSimulator) Pause() {
	ts.paused.Store(true)
}

// Resume resumes trading
func (ts *TradingSimulator) Resume() {
	ts.paused.Store(false)
}

// IsPaused returns whether trading is paused
func (ts *TradingSimulator) IsPaused() bool {
	return ts.paused.Load()
}

// abs returns absolute value
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
