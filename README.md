# watch

`watch` is a reactive framework for Go. It runs a managed function, tracks values from polling, flow, and tick sources, and supervises shutdown and recovery.

This project is currently released as `v0`. Public APIs may change before `v1`.

## Install

```bash
go get github.com/HershyOrg/watch@v0.2.1
```

The recommended public API starts with the root package:

```go
import "github.com/HershyOrg/watch"
```

Some watcher setup functions currently require types from `shared` and `wm`; import those subpackages only where their type signatures are needed.

## 30 second quickstart

This example declares BTCUSDT spot and futures mark prices as independent watched values, then derives the spot/futures gap inside the managed function. It does not place real orders; it records a paper position in `ManageContext`.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

const (
	symbol          = "BTCUSDT"
	spotPriceURL    = "https://api.binance.com/api/v3/ticker/price?symbol=" + symbol
	futuresMarkURL  = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + symbol
	entryGapBps     = 50.0
	exitGapBps      = 10.0
)

type GapSnapshot struct {
	SpotPrice        float64
	FuturesMarkPrice float64
	GapBps           float64
	ObservedAt       time.Time
}

type StrategyState struct {
	Position   string
	EntryCount int
	LastGapBps float64
}

func main() {
	config := watch.DefaultWatcherConfig()
	config.DisableAPIServer = true
	config.DefaultTimeout = 10 * time.Second

	w, err := watch.NewWatcher(config)
	if err != nil {
		panic(err)
	}

	w.Manage(func(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
		client := watch.Memo(func() *http.Client {
			return &http.Client{Timeout: 5 * time.Second}
		}, "binance_http", ctx)

		spot := watch.WatchCall[float64](
			0,
			func(callCtx wm.CallContext) (func(runCtx wm.RunContext) wm.UpdateFunc[float64], error) {
				return func(runCtx wm.RunContext) wm.UpdateFunc[float64] {
					price, err := binancePrice(runCtx, client, spotPriceURL, "price")

					return func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
						if err != nil {
							return shared.WatchValue[float64]{Error: err}, false
						}
						return shared.WatchValue[float64]{Value: price}, false
					}
				}, nil
			},
			"btcusdt_spot_price",
			5*time.Second,
			ctx,
		)

		futuresMark := watch.WatchCall[float64](
			0,
			func(callCtx wm.CallContext) (func(runCtx wm.RunContext) wm.UpdateFunc[float64], error) {
				return func(runCtx wm.RunContext) wm.UpdateFunc[float64] {
					price, err := binancePrice(runCtx, client, futuresMarkURL, "markPrice")

					return func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
						if err != nil {
							return shared.WatchValue[float64]{Error: err}, false
						}
						return shared.WatchValue[float64]{Value: price}, false
					}
				}, nil
			},
			"btcusdt_futures_mark_price",
			5*time.Second,
			ctx,
		)

		if msg != nil && msg.Content == "stop" {
			return watch.Stop("user requested stop"), nil
		}
		if spot.IsError() {
			return watch.None(), spot.Error
		}
		if futuresMark.IsError() {
			return watch.None(), futuresMark.Error
		}
		if !spot.IsUpdatedValid() || !futuresMark.IsUpdatedValid() {
			return watch.None(), nil
		}
		if !spot.IsTriggered(ctx) && !futuresMark.IsTriggered(ctx) {
			return watch.None(), nil
		}

		gap := GapSnapshot{
			SpotPrice:        spot.Value,
			FuturesMarkPrice: futuresMark.Value,
			GapBps:           (futuresMark.Value - spot.Value) / spot.Value * 10_000,
			ObservedAt:       time.Now(),
		}

		before, _ := ctx.GetValue("strategy").(StrategyState)
		after := ctx.UpdateValue("strategy", func(current any) any {
			state, _ := current.(StrategyState)
			if state.Position == "" {
				state.Position = "flat"
			}
			state.LastGapBps = gap.GapBps

			switch {
			case state.Position == "flat" && gap.GapBps >= entryGapBps:
				state.Position = "long_spot_short_future"
				state.EntryCount++
			case state.Position == "long_spot_short_future" && gap.GapBps <= exitGapBps:
				state.Position = "flat"
			}
			return state
		}).(StrategyState)

		fmt.Printf(
			"%s spot=%.2f futuresMark=%.2f gap=%.2fbps position=%s\n",
			gap.ObservedAt.Format(time.RFC3339),
			gap.SpotPrice,
			gap.FuturesMarkPrice,
			gap.GapBps,
			after.Position,
		)
		if normalizePosition(before.Position) != after.Position {
			fmt.Printf("paper transition: %s -> %s\n", normalizePosition(before.Position), after.Position)
		}

		return watch.None(), nil
	}, "binance_gap", nil)

	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	state, err := w.Run(runCtx)
	if err != nil {
		panic(err)
	}
	fmt.Println(state)
}

func binancePrice(ctx context.Context, client *http.Client, url, field string) (float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("binance returned %s", resp.Status)
	}

	var payload map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, err
	}

	raw := payload[field]
	if raw == "" {
		return 0, fmt.Errorf("binance response missing %q", field)
	}
	return strconv.ParseFloat(raw, 64)
}

func normalizePosition(position string) string {
	if position == "" {
		return "flat"
	}
	return position
}
```

The important part is the shape: market variables are declared independently with `WatchCall`, watch stores their latest state, and the managed function derives `gapBps` from those values before updating strategy state with `ctx.UpdateValue`. If the source is a WebSocket stream, keep the same `spot`/`futuresMark` declarations and replace those bodies with `WatchFlow`.

## Demos

The existing `demo/` module is for local development and uses:

```go
replace github.com/HershyOrg/watch => ../
```

That keeps local framework changes visible immediately.

`version_demo/` is separate. It has no local `replace`, requires `github.com/HershyOrg/watch v0.2.1`, prints the linked watch module version, and runs a small `WatchTick` smoke test.

```bash
cd version_demo
go run .
```

## Core API

### Watcher and Manage

`Watcher` owns the framework lifecycle. Register one managed function with `Manage`, then call `Run` or `Start`.

```go
config := watch.DefaultWatcherConfig()
config.DisableAPIServer = true

w, err := watch.NewWatcher(config)
if err != nil {
	return err
}

w.Manage(func(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
	if msg != nil && msg.Content == "stop" {
		return watch.Stop("user requested stop"), nil
	}
	return watch.None(), nil
}, "worker", nil)

_, err = w.Run(ctx)
return err
```

### WatchCall

`WatchCall` polls a value on a fixed interval and returns the latest value from framework state.

```go
import (
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

func managed(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
	price := watch.WatchCall[float64](
		0,
		func(callCtx wm.CallContext) (func(runCtx wm.RunContext) wm.UpdateFunc[float64], error) {
			return func(runCtx wm.RunContext) wm.UpdateFunc[float64] {
				value := float64(time.Now().Unix())

				return func(prev shared.WatchValue[float64]) (shared.WatchValue[float64], bool) {
					return shared.WatchValue[float64]{Value: value}, false
				}
			}, nil
		},
		"price",
		time.Second,
		ctx,
	)

	if price.IsUpdatedValid() {
		_ = price.Value
	}
	return watch.None(), nil
}
```

### WatchFlow

`WatchFlow` reads update functions from a channel. Flow goroutines should stop when `flowCtx.Done()` is closed.

```go
import (
	"time"

	"github.com/HershyOrg/watch"
	"github.com/HershyOrg/watch/shared"
	"github.com/HershyOrg/watch/wm"
)

func managed(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
	count := watch.WatchFlow[int](
		0,
		func(flowCtx wm.FlowContext) (chan wm.UpdateFunc[int], error) {
			out := make(chan wm.UpdateFunc[int], 16)

			go func() {
				defer close(out)

				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()

				for {
					select {
					case <-flowCtx.Done():
						return
					case <-ticker.C:
						update := func(prev shared.WatchValue[int]) (shared.WatchValue[int], bool) {
							return shared.WatchValue[int]{Value: prev.Value + 1}, false
						}

						select {
						case <-flowCtx.Done():
							return
						case out <- update:
						}
					}
				}
			}()

			return out, nil
		},
		"counter",
		ctx,
	)

	if count.IsUpdatedValid() {
		_ = count.Value
	}
	return watch.None(), nil
}
```

### WatchTick

`WatchTick` is a convenience wrapper for interval ticks.

```go
func managed(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
	tick := watch.WatchTick("heartbeat", time.Second, ctx)
	if tick.IsUpdatedValid() {
		fmt.Println(tick.Value.TickCount, tick.Value.Time)
	}
	return watch.None(), nil
}
```

### ReadEnv

`ReadEnv` reads a string from a `.watch.env` file in the caller file's directory. It does not read process environment variables. Read failures panic as watch initialization failures.

```text
API_KEY=local-development-key
```

```go
apiKey := watch.ReadEnv("API_KEY")
_ = apiKey
```

## More documentation

See [guide.md](./guide.md) for deeper usage rules, cancellation guidance, timeout behavior, and `.watch.env` details.
