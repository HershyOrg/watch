package binance

import (
	"fmt"
	"net/http"
	"time"

	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
	"github.com/gorilla/websocket"
)

func NewDialer() *websocket.Dialer {
	return &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 45 * time.Second,
	}
}

// BTCPriceFlow is a SetupUpdateFuncChan callback that owns a BinanceStream for BTC.
// All watch-symbol usage is contained inside this body (WFC-01).
func BTCPriceFlow(flowCtx wm.FlowContext) (chan wm.UpdateFunc[float64], error) {
	stream := newBinanceStream(NewDialer())
	if err := stream.Connect(); err != nil {
		return nil, fmt.Errorf("binance connect: %w", err)
	}

	sourceCh, err := stream.GetBTCPriceStream()(flowCtx)
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
			case upd, ok := <-sourceCh:
				if !ok {
					return
				}
				val := upd
				fn := func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
					if val.Err != nil {
						return shared.WatchValue[float64]{Error: val.Err}, false
					}
					return shared.WatchValue[float64]{Value: val.Price}, false
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

// ETHPriceFlow is a SetupUpdateFuncChan callback that owns a BinanceStream for ETH.
// All watch-symbol usage is contained inside this body (WFC-01).
func ETHPriceFlow(flowCtx wm.FlowContext) (chan wm.UpdateFunc[float64], error) {
	stream := newBinanceStream(NewDialer())
	if err := stream.Connect(); err != nil {
		return nil, fmt.Errorf("binance connect: %w", err)
	}

	sourceCh, err := stream.GetETHPriceStream()(flowCtx)
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
			case upd, ok := <-sourceCh:
				if !ok {
					return
				}
				val := upd
				fn := func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
					if val.Err != nil {
						return shared.WatchValue[float64]{Error: val.Err}, false
					}
					return shared.WatchValue[float64]{Value: val.Price}, false
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
