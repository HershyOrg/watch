package main

import (
	"fmt"
	"math/rand"
	"time"
)

// MarketClient simulates a Polymarket API client
type MarketClient struct {
	apiKey      string
	connected   bool
	lastRequest time.Time
}

// NewMarketClient creates a new market client (expensive initialization)
func NewMarketClient(apiKey string) *MarketClient {
	fmt.Println("  [MarketClient] Initializing connection...")
	time.Sleep(2000 * time.Millisecond) // Simulate connection delay

	client := &MarketClient{
		apiKey:      apiKey,
		connected:   true,
		lastRequest: time.Now(),
	}

	fmt.Println("  [MarketClient] ✓ Connected successfully")
	return client
}

// GetBitcoinPrice simulates fetching Bitcoin price from market
func (mc *MarketClient) GetBitcoinPrice() (float64, error) {
	if !mc.connected {
		return 0, fmt.Errorf("client not connected")
	}

	mc.lastRequest = time.Now()

	// Simulate Bitcoin price with some volatility
	basePrice := 45000.0
	volatility := rand.Float64()*2000 - 1000 // -1000 to +1000
	price := basePrice + volatility

	return price, nil
}

// GetMarketDepth simulates fetching market depth
func (mc *MarketClient) GetMarketDepth(market string) (map[string]float64, error) {
	if !mc.connected {
		return nil, fmt.Errorf("client not connected")
	}

	mc.lastRequest = time.Now()

	return map[string]float64{
		"bid":    0.52,
		"ask":    0.54,
		"volume": 125000.0,
	}, nil
}

// PlaceOrder simulates placing an order
func (mc *MarketClient) PlaceOrder(side string, amount float64) error {
	if !mc.connected {
		return fmt.Errorf("client not connected")
	}

	fmt.Printf("  [MarketClient] 📈 Placing %s order: $%.2f\n", side, amount)
	mc.lastRequest = time.Now()

	// Simulate order processing
	time.Sleep(50 * time.Millisecond)

	fmt.Println("  [MarketClient] ✓ Order executed")
	return nil
}

// Close simulates closing the connection
func (mc *MarketClient) Close() error {
	if !mc.connected {
		return nil
	}

	fmt.Println("  [MarketClient] Closing connection...")
	mc.connected = false

	// Simulate cleanup delay
	time.Sleep(100 * time.Millisecond)

	fmt.Println("  [MarketClient] ✓ Connection closed")
	return nil
}

// GetStats returns client statistics
func (mc *MarketClient) GetStats() string {
	return fmt.Sprintf("Connected: %v, Last Request: %s",
		mc.connected, mc.lastRequest.Format("15:04:05"))
}
