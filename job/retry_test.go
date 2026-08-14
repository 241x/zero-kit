package job

import (
	"testing"
	"time"
)

func TestExecutor_retryDelayExponential(t *testing.T) {
	e := &Executor{config: Config{
		RetryStrategy: RetryStrategyExponential,
		RetryDelay:    5 * time.Second,
		RetryMaxDelay: time.Hour,
	}}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{4, 40 * time.Second},
		{5, 80 * time.Second},
	}
	for _, tt := range tests {
		if got := e.retryDelay(tt.attempt); got != tt.want {
			t.Errorf("retryDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestExecutor_retryDelayFixed(t *testing.T) {
	e := &Executor{config: Config{
		RetryStrategy: RetryStrategyFixed,
		RetryDelay:    5 * time.Second,
	}}
	if got := e.retryDelay(10); got != 5*time.Second {
		t.Errorf("got %v, want 5s", got)
	}
}

func TestExecutor_retryDelayCapped(t *testing.T) {
	e := &Executor{config: Config{
		RetryStrategy: RetryStrategyExponential,
		RetryDelay:    5 * time.Second,
		RetryMaxDelay: 20 * time.Second,
	}}
	if got := e.retryDelay(100); got != 20*time.Second {
		t.Errorf("got %v, want cap 20s", got)
	}
}

func TestExecutor_retryDelayZeroMax(t *testing.T) {
	// RetryMaxDelay 为 0 视为无上限
	e := &Executor{config: Config{
		RetryStrategy: RetryStrategyExponential,
		RetryDelay:    5 * time.Second,
		RetryMaxDelay: 0,
	}}
	if got := e.retryDelay(4); got != 40*time.Second {
		t.Errorf("got %v, want 40s", got)
	}
}

func TestExecutor_retryDelayZeroBase(t *testing.T) {
	e := &Executor{config: Config{
		RetryStrategy: RetryStrategyExponential,
		RetryDelay:    0,
		RetryMaxDelay: time.Hour,
	}}
	if got := e.retryDelay(5); got != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestExecutor_retryDelayJittered(t *testing.T) {
	e := &Executor{config: Config{
		RetryStrategy: RetryStrategyFixed,
		RetryDelay:    10 * time.Second,
		RetryJitter:   true,
	}}

	// 抖动结果在 [0, delay) 范围内且有波动
	seen := make(map[time.Duration]struct{})
	for range 200 {
		d := e.retryDelayJittered(1)
		if d < 0 || d >= 10*time.Second {
			t.Fatalf("jittered delay %v out of range [0, 10s)", d)
		}
		seen[d] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatalf("jittered delay did not vary, got only %v", seen)
	}

	// 关闭抖动时返回精确延迟
	e.config.RetryJitter = false
	if got := e.retryDelayJittered(1); got != 10*time.Second {
		t.Fatalf("got %v, want 10s", got)
	}

	// 基础延迟为 0 时抖动仍返回 0
	e.config.RetryJitter = true
	e.config.RetryDelay = 0
	if got := e.retryDelayJittered(1); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
}
