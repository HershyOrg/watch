package wm

import "time"

// LoopRecoveryPolicy는 WatchLoop의 리커버리 정책임.
type LoopRecoveryPolicy struct {
	// MinConsecutiveFailures — Tier 2(전체 복구) 진입 기준
	MinConsecutiveFailures int
	// MaxConsecutiveFailures — Crash 기준
	MaxConsecutiveFailures int
	// MaxGetHandleRetries — GetHandle 재시도 횟수
	MaxGetHandleRetries int
	// BaseRetryDelay — Tier 2 기본 딜레이
	BaseRetryDelay time.Duration
	// MaxRetryDelay — 딜레이 상한
	MaxRetryDelay time.Duration
	// LightweightRetryDelays — Tier 1 딜레이 (인덱스별)
	LightweightRetryDelays []time.Duration
}

func DefaultLoopRecoveryPolicy() LoopRecoveryPolicy {
	return LoopRecoveryPolicy{
		MinConsecutiveFailures: 3,
		MaxConsecutiveFailures: 6,
		MaxGetHandleRetries:    3,
		BaseRetryDelay:         1 * time.Second,
		MaxRetryDelay:          30 * time.Second,
		LightweightRetryDelays: []time.Duration{
			500 * time.Millisecond,
			1 * time.Second,
			2 * time.Second,
		},
	}
}

// CalculateBackoff는 연속 에러 수에 따른 지수 백오프 딜레이를 계산함.
func (p *LoopRecoveryPolicy) CalculateBackoff(consecutiveErrs int) time.Duration {
	delay := p.BaseRetryDelay
	recoveryAttempts := consecutiveErrs - p.MinConsecutiveFailures
	if recoveryAttempts < 0 {
		recoveryAttempts = 0
	}
	for i := 0; i < recoveryAttempts; i++ {
		delay *= 2
		if delay > p.MaxRetryDelay {
			return p.MaxRetryDelay
		}
	}
	return delay
}

// LightweightDelay는 Tier 1 딜레이를 반환함.
func (p *LoopRecoveryPolicy) LightweightDelay(consecutiveErrs int) time.Duration {
	if len(p.LightweightRetryDelays) == 0 {
		return 0
	}
	idx := consecutiveErrs - 1
	if idx < 0 {
		return 0
	}
	if idx >= len(p.LightweightRetryDelays) {
		idx = len(p.LightweightRetryDelays) - 1
	}
	return p.LightweightRetryDelays[idx]
}
