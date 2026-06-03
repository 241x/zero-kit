package queue_test

import (
	"testing"
	"time"

	"github.com/241x/zero-kit/queue"

	"github.com/stretchr/testify/assert"
)

// TestDefaultConfig 验证默认配置的各项值符合预期
func TestDefaultConfig(t *testing.T) {
	config := queue.DefaultConfig()

	assert.Equal(t, "default", config.Name)
	assert.Equal(t, 10, config.MaxConcurrency)
	assert.Equal(t, 300, config.VisibilityTimeout)
	assert.Equal(t, 3, config.MaxRetries)
	assert.Equal(t, queue.RetryDelayExponential, config.RetryDelay)
	assert.True(t, config.EnableDeadLetter)
	assert.Equal(t, 3, config.DeadLetterMaxRetries)
	assert.Equal(t, time.Second, config.DelayQueueScanInterval)
	assert.Equal(t, time.Minute, config.ProcessingCleanupInterval)
}

// TestConfigWithName 验证设置队列名称
func TestConfigWithName(t *testing.T) {
	config := queue.DefaultConfig().WithName("email-queue")
	assert.Equal(t, "email-queue", config.Name)
}

// TestConfigWithMaxConcurrency 验证设置最大并发数
func TestConfigWithMaxConcurrency(t *testing.T) {
	config := queue.DefaultConfig().WithMaxConcurrency(5)
	assert.Equal(t, 5, config.MaxConcurrency)
}

// TestConfigWithVisibilityTimeout 验证设置可见性超时
func TestConfigWithVisibilityTimeout(t *testing.T) {
	config := queue.DefaultConfig().WithVisibilityTimeout(60)
	assert.Equal(t, 60, config.VisibilityTimeout)
}

// TestConfigWithMaxRetries 验证设置最大重试次数
func TestConfigWithMaxRetries(t *testing.T) {
	config := queue.DefaultConfig().WithMaxRetries(10)
	assert.Equal(t, 10, config.MaxRetries)
}

// TestConfigWithRetryDelay 验证设置重试延迟策略
func TestConfigWithRetryDelay(t *testing.T) {
	config := queue.DefaultConfig().WithRetryDelay(queue.RetryDelayFixed)
	assert.Equal(t, queue.RetryDelayFixed, config.RetryDelay)
}

// TestConfigWithDeadLetter 验证设置死信队列配置
func TestConfigWithDeadLetter(t *testing.T) {
	config := queue.DefaultConfig().WithDeadLetter(false, 5)
	assert.False(t, config.EnableDeadLetter)
	assert.Equal(t, 5, config.DeadLetterMaxRetries)
}

// TestConfigWithScanIntervals 验证设置扫描间隔
func TestConfigWithScanIntervals(t *testing.T) {
	config := queue.DefaultConfig().WithScanIntervals(5*time.Second, 10*time.Minute)
	assert.Equal(t, 5*time.Second, config.DelayQueueScanInterval)
	assert.Equal(t, 10*time.Minute, config.ProcessingCleanupInterval)
}

// TestConfigChainedCalls 验证多个 With 方法的链式调用结果正确
func TestConfigChainedCalls(t *testing.T) {
	config := queue.DefaultConfig().
		WithName("test-queue").
		WithMaxConcurrency(3).
		WithVisibilityTimeout(120).
		WithMaxRetries(5).
		WithRetryDelay(queue.RetryDelayFixed).
		WithDeadLetter(true, 10).
		WithScanIntervals(2*time.Second, 5*time.Minute)

	assert.Equal(t, "test-queue", config.Name)
	assert.Equal(t, 3, config.MaxConcurrency)
	assert.Equal(t, 120, config.VisibilityTimeout)
	assert.Equal(t, 5, config.MaxRetries)
	assert.Equal(t, queue.RetryDelayFixed, config.RetryDelay)
	assert.True(t, config.EnableDeadLetter)
	assert.Equal(t, 10, config.DeadLetterMaxRetries)
	assert.Equal(t, 2*time.Second, config.DelayQueueScanInterval)
	assert.Equal(t, 5*time.Minute, config.ProcessingCleanupInterval)
}

// TestConfigChainedCalls_NotModifyDefault 验证链式调用不会修改原始默认配置
func TestConfigChainedCalls_NotModifyDefault(t *testing.T) {
	original := queue.DefaultConfig()
	_ = original.WithName("modified").WithMaxConcurrency(1)

	fresh := queue.DefaultConfig()
	assert.Equal(t, "default", fresh.Name)
	assert.Equal(t, 10, fresh.MaxConcurrency)
}

// TestRetryDelayStrategyConstants 验证重试延迟策略常量值
func TestRetryDelayStrategyConstants(t *testing.T) {
	assert.Equal(t, queue.RetryDelayStrategy("fixed"), queue.RetryDelayFixed)
	assert.Equal(t, queue.RetryDelayStrategy("exponential"), queue.RetryDelayExponential)
	assert.Equal(t, queue.RetryDelayStrategy("random"), queue.RetryDelayRandom)
}
