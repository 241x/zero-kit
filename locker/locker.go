package locker

import (
	"context"
	"errors"
	"time"
)

// 错误定义
var (
	ErrLockAcquired = errors.New("lock is already acquired")
	ErrLockNotHeld  = errors.New("lock is not held")
	ErrInvalidToken = errors.New("invalid token")
)

// LockOption 锁选项函数类型
type LockOption func(*LockOptions)

// LockOptions 锁配置选项
type LockOptions struct {
	TTL      time.Duration // 锁过期时间
	WatchDog bool          // 是否启用看门狗自动续期
}

// defaultLockOptions 默认选项
func defaultLockOptions() *LockOptions {
	return &LockOptions{
		TTL:      30 * time.Second,
		WatchDog: false,
	}
}

// WithTTL 设置锁的过期时间
func WithTTL(ttl time.Duration) LockOption {
	return func(o *LockOptions) {
		o.TTL = ttl
	}
}

// WithWatchDog 启用看门狗自动续期
func WithWatchDog() LockOption {
	return func(o *LockOptions) {
		o.WatchDog = true
	}
}

// Locker 分布式锁接口
type Locker interface {
	// Lock 获取锁，返回锁对象和错误
	Lock(ctx context.Context, key string, opts ...LockOption) (Lock, error)

	// Unlock 释放锁（根据 key）
	Unlock(ctx context.Context, key string) error

	// Extend 延长锁的过期时间
	Extend(ctx context.Context, key string, ttl time.Duration) error
}

// Lock 单个锁实例接口
type Lock interface {
	// Key 获取锁的 key
	Key() string

	// Token 获取锁的唯一令牌（用于安全释放）
	Token() string

	// Unlock 释放锁
	Unlock(ctx context.Context) error

	// Extend 延长锁的过期时间
	Extend(ctx context.Context, ttl time.Duration) error
}
