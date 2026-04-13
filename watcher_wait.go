package watch

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"context"
)

// WaitReason describes why a Wait call returned.
type WaitReason int

const (
	// WaitReasonState indicates the desired state was reached.
	WaitReasonState WaitReason = iota
	// WaitReasonSignal indicates an OS signal was received (e.g., Ctrl+C).
	WaitReasonSignal
	// WaitReasonTimeout indicates the timeout expired.
	WaitReasonTimeout
	// WaitReasonContext indicates the external context was cancelled.
	WaitReasonContext
)

func (r WaitReason) String() string {
	switch r {
	case WaitReasonState:
		return "State"
	case WaitReasonSignal:
		return "Signal"
	case WaitReasonTimeout:
		return "Timeout"
	case WaitReasonContext:
		return "Context"
	default:
		return "Unknown"
	}
}

// WaitResult contains the outcome of a Wait call.
type WaitResult struct {
	State  ControlState // The state when Wait returned
	Reason WaitReason   // Why the wait ended
}

// --- WaitOption ---

// WaitOption configures Wait behavior.
type WaitOption func(*waitConfig)

type waitConfig struct {
	timeout      time.Duration
	ctx          context.Context
	signals      []os.Signal
	pollInterval time.Duration
}

func defaultWaitConfig() waitConfig {
	return waitConfig{
		pollInterval: 100 * time.Millisecond,
	}
}

// WithTimeout sets a maximum duration to wait.
func WithTimeout(d time.Duration) WaitOption {
	return func(c *waitConfig) {
		c.timeout = d
	}
}

// WithContext attaches an external context for cancellation.
// When the context is cancelled, StopAll() is called and Wait continues
// until a terminal state is reached.
func WithContext(ctx context.Context) WaitOption {
	return func(c *waitConfig) {
		c.ctx = ctx
	}
}

// WithInterrupt intercepts OS signals and calls StopAll() when received.
// If no signals are specified, defaults to SIGINT and SIGTERM.
// Wait continues until a terminal state is reached after the signal.
func WithInterrupt(signals ...os.Signal) WaitOption {
	return func(c *waitConfig) {
		if len(signals) == 0 {
			c.signals = []os.Signal{os.Interrupt, syscall.SIGTERM}
		} else {
			c.signals = signals
		}
	}
}

// --- Wait methods ---

// WaitForTerminal blocks until the Manager reaches a terminal state
// (Stopped, Killed, or Crashed).
// 시그널/컨텍스트 수신 시 StopAll()을 호출하되, 터미널 상태까지 계속 대기하여
// cleanup 완료를 보장한다.
func (w *Watcher) WaitForTerminal(opts ...WaitOption) WaitResult {
	cfg := defaultWaitConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return w.waitLoop(cfg, func(state ControlState) bool {
		return state.IsTerminal()
	})
}

// WaitForState blocks until the Manager reaches the specified state type.
// Comparison is by type (not value equality).
func (w *Watcher) WaitForState(target ControlState, opts ...WaitOption) WaitResult {
	cfg := defaultWaitConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	targetType := fmt.Sprintf("%T", target)
	return w.waitLoop(cfg, func(state ControlState) bool {
		return fmt.Sprintf("%T", state) == targetType
	})
}

// StartAndWait is a convenience method: StartAndRun() + WaitForTerminal(opts...).
// If StartAndRun fails, returns a zero WaitResult and the error.
func (w *Watcher) StartAndWait(opts ...WaitOption) (WaitResult, error) {
	if err := w.startAndRun(); err != nil {
		return WaitResult{}, err
	}
	return w.WaitForTerminal(opts...), nil
}

// --- internal ---

// waitLoop is the core select loop shared by WaitForTerminal and WaitForState.
func (w *Watcher) waitLoop(cfg waitConfig, match func(ControlState) bool) WaitResult {
	ticker := time.NewTicker(cfg.pollInterval)
	defer ticker.Stop()

	// Timeout channel
	var timeoutCh <-chan time.Time
	if cfg.timeout > 0 {
		timeoutCh = time.After(cfg.timeout)
	}

	// Signal channel
	var sigChan chan os.Signal
	if len(cfg.signals) > 0 {
		sigChan = make(chan os.Signal, 1)
		signal.Notify(sigChan, cfg.signals...)
		defer signal.Stop(sigChan)
	}

	// Context done channel
	var ctxDone <-chan struct{}
	if cfg.ctx != nil {
		ctxDone = cfg.ctx.Done()
	}

	reason := WaitReasonState

	for {
		select {
		case <-ticker.C:
			state := w.GetState()
			if match(state) {
				return WaitResult{State: state, Reason: reason}
			}

		case <-sigChan:
			sigChan = nil // 재진입 방지
			reason = WaitReasonSignal
			go w.StopAll()

		case <-ctxDone:
			ctxDone = nil // 재진입 방지
			if reason == WaitReasonState {
				reason = WaitReasonContext
			}
			go w.StopAll()

		case <-timeoutCh:
			return WaitResult{State: w.GetState(), Reason: WaitReasonTimeout}
		}
	}
}
