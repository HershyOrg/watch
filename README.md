# watch

`watch` is a reactive framework for Go. It runs a managed function, tracks values from polling, flow, and tick sources, and supervises shutdown and recovery.

This project is currently released as `v0`. Public APIs may change before `v1`.

## Install

```bash
go get github.com/HershyOrg/watch@v0.1.0
```

The recommended public API starts with the root package:

```go
import "github.com/HershyOrg/watch"
```

Some watcher setup functions currently require types from `shared` and `wm`; import those subpackages only where their type signatures are needed.

## 30 second quickstart

```go
package main

import (
	"context"
	"fmt"

	"github.com/HershyOrg/watch"
)

func main() {
	config := watch.DefaultWatcherConfig()
	config.DisableAPIServer = true

	w, err := watch.NewWatcher(config)
	if err != nil {
		panic(err)
	}

	w.Manage(func(msg *watch.Message, ctx watch.ManageContext) (watch.ControlSignal, error) {
		fmt.Println("hello from watch")
		return watch.Stop("example complete"), nil
	}, "hello", nil)

	state, err := w.Run(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(state)
}
```

## Demos

The existing `demo/` module is for local development and uses:

```go
replace github.com/HershyOrg/watch => ../
```

That keeps local framework changes visible immediately.

`version_demo/` is separate. It has no local `replace`, requires `github.com/HershyOrg/watch v0.1.0`, prints the linked watch module version, and runs a small `WatchTick` smoke test.

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
