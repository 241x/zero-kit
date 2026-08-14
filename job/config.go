package job

import (
	"errors"
	"time"
)

// RetryStrategy 重试退避策略
type RetryStrategy string

const (
	// RetryStrategyFixed 固定延迟
	RetryStrategyFixed RetryStrategy = "fixed"
	// RetryStrategyExponential 指数退避
	RetryStrategyExponential RetryStrategy = "exponential"
)

// Config 作业执行器配置
type Config struct {
	Queues            []string      // 要消费的队列名
	Concurrency       int           // 每队列并发执行数
	HeartbeatInterval time.Duration // 心跳间隔
	StaleTimeout      time.Duration // 心跳超时判定（超过则视为失联，重新入队）
	RecoverInterval   time.Duration // 失联作业扫描间隔
	JobTimeout        time.Duration // 单个作业执行超时（0 表示不限制）
	RetryDelay        time.Duration // 重试退避基础延迟
	RetryMaxDelay     time.Duration // 重试退避最大延迟
	RetryStrategy     RetryStrategy // 重试退避策略
	RetryJitter       bool          // 重试延迟加入随机抖动（默认开启，避免惊群）
	Retention         time.Duration // 终态作业保留时长（0 表示永久保留，不自动清理）
	CleanupInterval   time.Duration // 终态作业清理间隔（Retention > 0 时生效）
	ShutdownTimeout   time.Duration // 优雅停止等待处理器退出的超时（0 使用默认 30s）
	ProgressInterval  time.Duration // 进度上报节流间隔（合并高频上报，0 表示不节流）
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Queues:            []string{DefaultQueue},
		Concurrency:       10,
		HeartbeatInterval: 10 * time.Second,
		StaleTimeout:      60 * time.Second,
		RecoverInterval:   30 * time.Second,
		JobTimeout:        0,
		RetryDelay:        5 * time.Second,
		RetryMaxDelay:     time.Hour,
		RetryStrategy:     RetryStrategyExponential,
		RetryJitter:       true,
		Retention:         0,
		CleanupInterval:   time.Minute,
		ShutdownTimeout:   30 * time.Second,
		ProgressInterval:  time.Second,
	}
}

// WithQueues 设置要消费的队列名
func (c Config) WithQueues(queues ...string) Config {
	c.Queues = queues
	return c
}

// WithConcurrency 设置每队列并发执行数
func (c Config) WithConcurrency(n int) Config {
	c.Concurrency = n
	return c
}

// WithHeartbeatInterval 设置心跳间隔
func (c Config) WithHeartbeatInterval(interval time.Duration) Config {
	c.HeartbeatInterval = interval
	return c
}

// WithStaleTimeout 设置心跳超时判定
func (c Config) WithStaleTimeout(timeout time.Duration) Config {
	c.StaleTimeout = timeout
	return c
}

// WithRecoverInterval 设置失联扫描间隔
func (c Config) WithRecoverInterval(interval time.Duration) Config {
	c.RecoverInterval = interval
	return c
}

// WithJobTimeout 设置单个作业执行超时（0 表示不限制）
func (c Config) WithJobTimeout(timeout time.Duration) Config {
	c.JobTimeout = timeout
	return c
}

// WithRetryDelay 设置重试退避基础延迟
func (c Config) WithRetryDelay(delay time.Duration) Config {
	c.RetryDelay = delay
	return c
}

// WithRetryMaxDelay 设置重试退避最大延迟
func (c Config) WithRetryMaxDelay(delay time.Duration) Config {
	c.RetryMaxDelay = delay
	return c
}

// WithRetryStrategy 设置重试退避策略
func (c Config) WithRetryStrategy(strategy RetryStrategy) Config {
	c.RetryStrategy = strategy
	return c
}

// WithRetryJitter 设置重试延迟是否加入随机抖动（默认开启，避免惊群）
func (c Config) WithRetryJitter(jitter bool) Config {
	c.RetryJitter = jitter
	return c
}

// WithRetention 设置终态作业保留时长（0 表示永久保留）
func (c Config) WithRetention(d time.Duration) Config {
	c.Retention = d
	return c
}

// WithCleanupInterval 设置终态作业清理间隔
func (c Config) WithCleanupInterval(interval time.Duration) Config {
	c.CleanupInterval = interval
	return c
}

// WithShutdownTimeout 设置优雅停止等待处理器退出的超时（0 使用默认 30s）
func (c Config) WithShutdownTimeout(timeout time.Duration) Config {
	c.ShutdownTimeout = timeout
	return c
}

// WithProgressInterval 设置进度上报节流间隔（0 表示不节流，每次上报立即落库）
func (c Config) WithProgressInterval(interval time.Duration) Config {
	c.ProgressInterval = interval
	return c
}

// Validate 校验配置合法性，返回首个不合法的配置项错误。
// 零值或负值的周期配置会导致 ticker panic 或失联判定失效，因此必须在启动前校验。
func (c Config) Validate() error {
	if c.Concurrency <= 0 {
		return errors.New("job: concurrency must be greater than 0")
	}
	if c.HeartbeatInterval <= 0 {
		return errors.New("job: heartbeat interval must be greater than 0")
	}
	if c.StaleTimeout <= 0 {
		return errors.New("job: stale timeout must be greater than 0")
	}
	if c.StaleTimeout <= c.HeartbeatInterval {
		return errors.New("job: stale timeout must be greater than heartbeat interval")
	}
	if c.RecoverInterval <= 0 {
		return errors.New("job: recover interval must be greater than 0")
	}
	if c.RetryDelay < 0 {
		return errors.New("job: retry delay must not be negative")
	}
	if c.RetryMaxDelay < 0 {
		return errors.New("job: retry max delay must not be negative")
	}
	if c.Retention > 0 && c.CleanupInterval <= 0 {
		return errors.New("job: cleanup interval must be greater than 0 when retention is set")
	}
	if c.ShutdownTimeout < 0 {
		return errors.New("job: shutdown timeout must not be negative")
	}
	if c.ProgressInterval < 0 {
		return errors.New("job: progress interval must not be negative")
	}
	return nil
}
