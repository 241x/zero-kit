package locker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLocker 基于 Redis 的分布式锁实现
type RedisLocker struct {
	client *redis.Client
	prefix string
}

// NewRedisLocker 创建 Redis 分布式锁实例
func NewRedisLocker(client *redis.Client) *RedisLocker {
	return &RedisLocker{
		client: client,
		prefix: "lock:",
	}
}

// getKey 转换 key，添加前缀
func (l *RedisLocker) getKey(key string) string {
	return l.prefix + key
}

// redisLock Redis 锁实例
type redisLock struct {
	key      string // 用户传入的原始 key
	redisKey string // 带前缀的 Redis key
	token    string
	client   *redis.Client
	ttl      time.Duration
}

// Key 获取锁的 key（返回用户传入的原始 key）
func (l *redisLock) Key() string {
	return l.key
}

// Token 获取锁的唯一令牌
func (l *redisLock) Token() string {
	return l.token
}

// Unlock 释放锁
func (l *redisLock) Unlock(ctx context.Context) error {
	return l.unlockWithToken(ctx, l.token)
}

// Extend 延长锁的过期时间
func (l *redisLock) Extend(ctx context.Context, ttl time.Duration) error {
	return l.extendWithToken(ctx, l.token, ttl)
}

// Lock 获取分布式锁
func (l *RedisLocker) Lock(ctx context.Context, key string, opts ...LockOption) (Lock, error) {
	// 应用选项
	options := defaultLockOptions()
	for _, opt := range opts {
		opt(options)
	}

	// 生成令牌
	token := generateToken()

	// 尝试获取锁
	lock, err := l.tryLock(ctx, key, token, options.TTL)
	if err != nil {
		return nil, err
	}

	// 如果获取成功且启用了看门狗，启动看门狗
	if lock != nil && options.WatchDog {
		go l.startWatchDog(ctx, lock, options.TTL)
	}

	return lock, nil
}

// tryLock 尝试获取锁
func (l *RedisLocker) tryLock(ctx context.Context, key, token string, ttl time.Duration) (*redisLock, error) {
	fullKey := l.getKey(key)
	err := l.client.SetArgs(ctx, fullKey, token, redis.SetArgs{
		Mode: "nx",
		TTL:  ttl,
	}).Err()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrLockAcquired
		}
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	return &redisLock{
		key:      key,     // 存储原始 key
		redisKey: fullKey, // 存储带前缀的 key，用于 Redis 操作
		token:    token,
		client:   l.client,
		ttl:      ttl,
	}, nil
}

// Unlock 释放锁（根据 key）
// 注意：这种方式不安全，因为无法验证 token
// 建议使用 Lock 接口返回的 Lock 实例来释放
func (l *RedisLocker) Unlock(ctx context.Context, key string) error {
	return l.client.Del(ctx, l.getKey(key)).Err()
}

// Extend 延长锁的过期时间
// 注意：这种方式不安全，因为无法验证 token
// 建议使用 Lock 接口返回的 Lock 实例来延长
func (l *RedisLocker) Extend(ctx context.Context, key string, ttl time.Duration) error {
	fullKey := l.getKey(key)
	exists, err := l.client.Exists(ctx, fullKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return ErrLockNotHeld
	}
	return l.client.Expire(ctx, fullKey, ttl).Err()
}

// unlockWithToken 使用令牌释放锁（安全释放）
func (l *redisLock) unlockWithToken(ctx context.Context, token string) error {
	// Lua 脚本：只有 key 对应的值等于 token 才删除
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, l.client, []string{l.redisKey}, token).Int()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if result == 0 {
		return ErrInvalidToken
	}
	return nil
}

// extendWithToken 使用令牌延长锁的过期时间
func (l *redisLock) extendWithToken(ctx context.Context, token string, ttl time.Duration) error {
	// Lua 脚本：只有 key 对应的值等于 token 才延长过期时间
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, l.client, []string{l.redisKey}, token, ttl.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("failed to extend lock: %w", err)
	}
	if result == 0 {
		return ErrInvalidToken
	}
	return nil
}

// startWatchDog 启动看门狗，自动续期
func (l *RedisLocker) startWatchDog(ctx context.Context, lock *redisLock, ttl time.Duration) {
	// 每隔 TTL/3 时间续期一次
	renewInterval := max(ttl/3, time.Second)

	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 检查锁是否还存在
			exists, err := lock.client.Exists(ctx, lock.redisKey).Result()
			if err != nil || exists == 0 {
				return // 锁已释放或过期
			}

			// 尝试续期
			err = lock.Extend(ctx, ttl)
			if err != nil {
				// 续期失败，可能是锁已被释放
				return
			}
		}
	}
}

// generateToken 生成唯一的随机令牌
func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
