package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HershyOrg/watch/shared"
	"github.com/gorilla/websocket"
)

// BinanceStream handles WebSocket connection to Binance for real-time price data
type BinanceStream struct {
	// Internal channels for message distribution
	btcInternalChan chan shared.FlowValue[float64]
	ethInternalChan chan shared.FlowValue[float64]

	// Current prices (atomic access)
	currentBTC atomic.Value // float64
	currentETH atomic.Value // float64

	// Statistics
	stats struct {
		messagesReceived atomic.Int64
		reconnects       atomic.Int64
		lastUpdate       atomic.Value // time.Time
		errors           atomic.Int64
	}

	// Connection state
	connected atomic.Bool
	wsConn    *websocket.Conn
	mu        sync.RWMutex

	// Control
	stopChan chan struct{}
	stopped  atomic.Bool
}

// BinanceTradeMsg represents a Binance trade message
type BinanceTradeMsg struct {
	Stream string `json:"stream"`
	Data   struct {
		Event     string `json:"e"` // Event type
		EventTime int64  `json:"E"` // Event time
		Symbol    string `json:"s"` // Symbol
		Price     string `json:"p"` // Price
		Quantity  string `json:"q"` // Quantity
		TradeTime int64  `json:"T"` // Trade time
	} `json:"data"`
}

// NewBinanceStream creates a new Binance WebSocket stream client
func NewBinanceStream() *BinanceStream {
	bs := &BinanceStream{
		btcInternalChan: make(chan shared.FlowValue[float64], 100),
		ethInternalChan: make(chan shared.FlowValue[float64], 100),
		stopChan:        make(chan struct{}),
	}

	bs.currentBTC.Store(0.0)
	bs.currentETH.Store(0.0)
	bs.stats.lastUpdate.Store(time.Now())

	return bs
}

// Connect establishes WebSocket connection to Binance
func (bs *BinanceStream) Connect() error {
	if bs.stopped.Load() {
		return fmt.Errorf("stream already stopped")
	}

	if bs.connected.Load() {
		return nil // Already connected
	}

	url := "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to Binance: %w", err)
	}

	bs.mu.Lock()
	bs.wsConn = conn
	bs.mu.Unlock()

	bs.connected.Store(true)

	// Start SINGLE receiveLoop that distributes to internal channels
	go bs.receiveLoop()

	return nil
}

// receiveLoop continuously receives messages from WebSocket and distributes to internal channels
func (bs *BinanceStream) receiveLoop() {
	defer func() {
		bs.connected.Store(false)
		// Close internal channels when stream stops
		if bs.stopped.Load() {
			close(bs.btcInternalChan)
			close(bs.ethInternalChan)
		}
	}()

	// Error injection: every 5 seconds, inject a simulated error
	lastErrorInjection := time.Now()
	errorInjectionInterval := 5 * time.Second

	for {
		select {
		case <-bs.stopChan:
			return
		default:
			// Continue receiving
		}

		// Inject simulated error every 5 seconds for demo purposes
		if time.Since(lastErrorInjection) > errorInjectionInterval {
			errValue := shared.FlowValue[float64]{V: 0, E: fmt.Errorf("simulated error injection (every 5s)")}
			select {
			case bs.btcInternalChan <- errValue:
			default:
			}
			select {
			case bs.ethInternalChan <- errValue:
			default:
			}
			lastErrorInjection = time.Now()
		}

		bs.mu.RLock()
		conn := bs.wsConn
		bs.mu.RUnlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Set read deadline to detect disconnections
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		var msg BinanceTradeMsg
		err := conn.ReadJSON(&msg)
		if err != nil {
			if bs.stopped.Load() {
				return
			}

			bs.stats.errors.Add(1)

			// Send error to BOTH internal channels
			errValue := shared.FlowValue[float64]{V: 0, E: fmt.Errorf("read error: %w", err)}
			select {
			case bs.btcInternalChan <- errValue:
			default:
			}
			select {
			case bs.ethInternalChan <- errValue:
			default:
			}

			// Attempt reconnection
			bs.reconnect()
			continue
		}

		// Process message and distribute to correct internal channel
		bs.processMessage(msg)
		bs.stats.messagesReceived.Add(1)
		bs.stats.lastUpdate.Store(time.Now())
	}
}

// processMessage parses and distributes price updates to internal channels
func (bs *BinanceStream) processMessage(msg BinanceTradeMsg) {
	var price float64
	fmt.Sscanf(msg.Data.Price, "%f", &price)

	if price == 0 {
		return
	}

	flowValue := shared.FlowValue[float64]{V: price, E: nil}

	// Update atomic value and send to correct internal channel
	switch msg.Stream {
	case "btcusdt@trade":
		bs.currentBTC.Store(price)
		select {
		case bs.btcInternalChan <- flowValue:
		default:
			// Channel full, skip
		}

	case "ethusdt@trade":
		bs.currentETH.Store(price)
		select {
		case bs.ethInternalChan <- flowValue:
		default:
			// Channel full, skip
		}
	}
}

// reconnect attempts to reconnect to Binance WebSocket
func (bs *BinanceStream) reconnect() {
	if bs.stopped.Load() {
		return
	}

	fmt.Println("[Stream] Reconnecting...")
	bs.stats.reconnects.Add(1)

	// Close old connection
	bs.mu.Lock()
	if bs.wsConn != nil {
		bs.wsConn.Close()
		bs.wsConn = nil
	}
	bs.mu.Unlock()

	bs.connected.Store(false)

	// Wait before reconnecting
	time.Sleep(2 * time.Second)

	// Try to reconnect
	err := bs.Connect()
	if err != nil {
		fmt.Printf("[Stream] Reconnection failed: %v\n", err)
	} else {
		fmt.Println("[Stream] Reconnected successfully")
	}
}

// GetBTCPriceStream returns a function that creates a BTC price channel for WatchFlow
func (bs *BinanceStream) GetBTCPriceStream() func(ctx context.Context) (<-chan shared.FlowValue[float64], error) {
	return func(ctx context.Context) (<-chan shared.FlowValue[float64], error) {
		if bs.stopped.Load() {
			return nil, fmt.Errorf("stream already stopped")
		}

		// Check if connected
		if !bs.connected.Load() {
			return nil, fmt.Errorf("stream not connected - call Connect() first")
		}

		// Create subscriber channel
		subscriberChan := make(chan shared.FlowValue[float64], 100)

		// Forward from internal channel to subscriber
		go func() {
			defer close(subscriberChan)
			for {
				select {
				case <-ctx.Done():
					return
				case value, ok := <-bs.btcInternalChan:
					if !ok {
						return
					}
					select {
					case subscriberChan <- value:
					case <-ctx.Done():
						return
					}
				}
			}
		}()

		return subscriberChan, nil
	}
}

// GetETHPriceStream returns a function that creates an ETH price channel for WatchFlow
func (bs *BinanceStream) GetETHPriceStream() func(ctx context.Context) (<-chan shared.FlowValue[float64], error) {
	return func(ctx context.Context) (<-chan shared.FlowValue[float64], error) {
		if bs.stopped.Load() {
			return nil, fmt.Errorf("stream already stopped")
		}

		// Check if connected
		if !bs.connected.Load() {
			return nil, fmt.Errorf("stream not connected - call Connect() first")
		}

		// Create subscriber channel
		subscriberChan := make(chan shared.FlowValue[float64], 100)

		// Forward from internal channel to subscriber
		go func() {
			defer close(subscriberChan)
			for {
				select {
				case <-ctx.Done():
					return
				case value, ok := <-bs.ethInternalChan:
					if !ok {
						return
					}
					select {
					case subscriberChan <- value:
					case <-ctx.Done():
						return
					}
				}
			}
		}()

		return subscriberChan, nil
	}
}

// GetCurrentBTC returns the current BTC price
func (bs *BinanceStream) GetCurrentBTC() float64 {
	if v := bs.currentBTC.Load(); v != nil {
		return v.(float64)
	}
	return 0
}

// GetCurrentETH returns the current ETH price
func (bs *BinanceStream) GetCurrentETH() float64 {
	if v := bs.currentETH.Load(); v != nil {
		return v.(float64)
	}
	return 0
}

// GetStats returns stream statistics
func (bs *BinanceStream) GetStats() StreamStats {
	lastUpdate := bs.stats.lastUpdate.Load().(time.Time)

	return StreamStats{
		MessagesReceived: bs.stats.messagesReceived.Load(),
		Reconnects:       bs.stats.reconnects.Load(),
		Errors:           bs.stats.errors.Load(),
		LastUpdate:       lastUpdate,
		Connected:        bs.connected.Load(),
	}
}

// StreamStats contains WebSocket stream statistics
type StreamStats struct {
	MessagesReceived int64
	Reconnects       int64
	Errors           int64
	LastUpdate       time.Time
	Connected        bool
}

// Close gracefully closes the WebSocket connection
func (bs *BinanceStream) Close() error {
	if !bs.stopped.CompareAndSwap(false, true) {
		return nil // Already stopped
	}

	fmt.Println("[Stream] Closing WebSocket connection...")

	// Signal stop
	close(bs.stopChan)

	// Close WebSocket connection
	bs.mu.Lock()
	if bs.wsConn != nil {
		bs.wsConn.Close()
		bs.wsConn = nil
	}
	bs.mu.Unlock()

	bs.connected.Store(false)

	fmt.Println("[Stream] WebSocket closed")
	return nil
}
