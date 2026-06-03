package queue

import (
	"time"
)

// QueueConfig 队列配置
type QueueConfig struct {
	Name                      string             // 队列名称
	MaxConcurrency            int                // 最大并发工作线程数
	VisibilityTimeout         int                // 任务可见性超时（秒）
	MaxRetries                int                // 最大重试次数
	RetryDelay                RetryDelayStrategy // 重试延迟策略
	EnableDeadLetter          bool               // 是否启用死信队列
	DeadLetterMaxRetries      int                // 死信队列最大重试次数
	DelayQueueScanInterval    time.Duration      // 延迟队列扫描间隔
	ProcessingCleanupInterval time.Duration      // 处理中任务清理间隔
}

// RetryDelayStrategy 重试延迟策略
type RetryDelayStrategy string

const (
	// RetryDelayFixed 固定延迟
	RetryDelayFixed RetryDelayStrategy = "fixed"
	// RetryDelayExponential 指数退避
	RetryDelayExponential RetryDelayStrategy = "exponential"
	// RetryDelayRandom 随机延迟
	RetryDelayRandom RetryDelayStrategy = "random"
)

// DefaultConfig 返回默认队列配置
func DefaultConfig() QueueConfig {
	return QueueConfig{
		Name:                      "default",
		MaxConcurrency:            10,
		VisibilityTimeout:         300, // 5分钟
		MaxRetries:                3,
		RetryDelay:                RetryDelayExponential,
		EnableDeadLetter:          true,
		DeadLetterMaxRetries:      3,
		DelayQueueScanInterval:    time.Second,
		ProcessingCleanupInterval: time.Minute,
	}
}

// WithName 设置队列名称
func (c QueueConfig) WithName(name string) QueueConfig {
	c.Name = name
	return c
}

// WithMaxConcurrency 设置最大并发数
func (c QueueConfig) WithMaxConcurrency(max int) QueueConfig {
	c.MaxConcurrency = max
	return c
}

// WithVisibilityTimeout 设置可见性超时
func (c QueueConfig) WithVisibilityTimeout(timeout int) QueueConfig {
	c.VisibilityTimeout = timeout
	return c
}

// WithMaxRetries 设置最大重试次数
func (c QueueConfig) WithMaxRetries(retries int) QueueConfig {
	c.MaxRetries = retries
	return c
}

// WithRetryDelay 设置重试延迟策略
func (c QueueConfig) WithRetryDelay(strategy RetryDelayStrategy) QueueConfig {
	c.RetryDelay = strategy
	return c
}

// WithDeadLetter 设置死信队列配置
func (c QueueConfig) WithDeadLetter(enable bool, maxRetries int) QueueConfig {
	c.EnableDeadLetter = enable
	c.DeadLetterMaxRetries = maxRetries
	return c
}

// WithScanIntervals 设置扫描间隔
func (c QueueConfig) WithScanIntervals(delayScan, cleanup time.Duration) QueueConfig {
	c.DelayQueueScanInterval = delayScan
	c.ProcessingCleanupInterval = cleanup
	return c
}
