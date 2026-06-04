package locker_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/241x/zero-kit/locker"
)

// setupTestRedis 创建基于 miniredis 的测试 Redis 客户端
func setupTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	require.NoError(t, client.Ping(context.Background()).Err())

	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return client, cleanup
}

// TestRedisLocker_LockAndUnlock 测试基本的加锁和解锁
func TestRedisLocker_LockAndUnlock(t *testing.T) {
	client, cleanup := setupTestRedis(t)
	defer cleanup()

	l := locker.NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:basic"

	// 获取锁
	lock, err := l.Lock(ctx, key, locker.WithTTL(10*time.Second))
	require.NoError(t, err)
	assert.NotNil(t, lock)
	assert.Equal(t, key, lock.Key())
	assert.NotEmpty(t, lock.Token())

	// 释放锁
	err = lock.Unlock(ctx)
	require.NoError(t, err)

	// 验证锁已释放
	exists, err := client.Exists(ctx, "lock:"+key).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

// TestRedisLocker_LockConflict 测试锁冲突
func TestRedisLocker_LockConflict(t *testing.T) {
	client, cleanup := setupTestRedis(t)
	defer cleanup()

	l := locker.NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:conflict"

	// 第一次获取锁
	lock1, err := l.Lock(ctx, key, locker.WithTTL(10*time.Second))
	require.NoError(t, err)

	// 第二次获取同一把锁应该失败
	lock2, err := l.Lock(ctx, key, locker.WithTTL(10*time.Second))
	assert.Error(t, err)
	assert.ErrorIs(t, err, locker.ErrLockAcquired)
	assert.Nil(t, lock2)

	// 释放第一把锁
	err = lock1.Unlock(ctx)
	require.NoError(t, err)

	// 现在可以获取锁了
	lock3, err := l.Lock(ctx, key, locker.WithTTL(10*time.Second))
	require.NoError(t, err)
	assert.NotNil(t, lock3)
	lock3.Unlock(ctx)
}

// TestRedisLocker_Extend 测试锁延期
func TestRedisLocker_Extend(t *testing.T) {
	client, cleanup := setupTestRedis(t)
	defer cleanup()

	l := locker.NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:extend"

	// 获取锁，过期时间 2 秒
	lock, err := l.Lock(ctx, key, locker.WithTTL(2*time.Second))
	require.NoError(t, err)

	// 延期到 10 秒
	err = lock.Extend(ctx, 10*time.Second)
	require.NoError(t, err)

	// 验证过期时间已延长
	ttl, err := client.TTL(ctx, "lock:"+key).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl.Seconds(), 5.0)

	lock.Unlock(ctx)
}

// TestRedisLocker_InvalidTokenUnlock 测试使用错误令牌解锁
func TestRedisLocker_InvalidTokenUnlock(t *testing.T) {
	client, cleanup := setupTestRedis(t)
	defer cleanup()

	l := locker.NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:invalid_token"

	// 获取锁
	lock, err := l.Lock(ctx, key, locker.WithTTL(10*time.Second))
	require.NoError(t, err)

	// 通过 raw Redis 将 token 覆盖为错误值，模拟 token 不匹配场景
	err = client.Set(ctx, "lock:"+key, "wrong_token", 0).Err()
	require.NoError(t, err)

	// 尝试用错误的 token 解锁
	err = lock.Unlock(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, locker.ErrInvalidToken)

	// 恢复正确的 token，验证锁仍能被正确解锁
	err = client.Set(ctx, "lock:"+key, lock.Token(), 0).Err()
	require.NoError(t, err)

	err = lock.Unlock(ctx)
	require.NoError(t, err)
}

// TestRedisLocker_ConcurrentLock 测试并发加锁
func TestRedisLocker_ConcurrentLock(t *testing.T) {
	client, cleanup := setupTestRedis(t)
	defer cleanup()

	l := locker.NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:concurrent"

	results := make(chan error, 10)

	// 10 个协程同时尝试获取同一把锁
	for range 10 {
		go func() {
			lock, err := l.Lock(ctx, key, locker.WithTTL(5*time.Second))
			if err != nil {
				results <- err
				return
			}
			time.Sleep(100 * time.Millisecond)
			results <- lock.Unlock(ctx)
		}()
	}

	// 等待所有结果
	successCount := 0
	failCount := 0
	for range 10 {
		err := <-results
		if err == nil {
			successCount++
		} else if err == locker.ErrLockAcquired {
			failCount++
		}
	}

	// 应该只有 1 个成功，其他 9 个失败
	assert.Equal(t, 1, successCount)
	assert.Equal(t, 9, failCount)
}

// TestGenerateToken 测试令牌生成
func TestGenerateToken(t *testing.T) {
	token1 := locker.GenerateToken()
	token2 := locker.GenerateToken()

	// 令牌长度应该是 32 字节的十六进制表示
	assert.Len(t, token1, 32)
	assert.Len(t, token2, 32)

	// 令牌应该唯一
	assert.NotEqual(t, token1, token2)
}

// TestLockOptions 测试选项函数
func TestLockOptions(t *testing.T) {
	opts := locker.DefaultLockOptions()
	assert.Equal(t, 30*time.Second, opts.TTL)
	assert.False(t, opts.WatchDog)

	// 应用自定义选项
	locker.WithTTL(60 * time.Second)(opts)
	locker.WithWatchDog()(opts)

	assert.Equal(t, 60*time.Second, opts.TTL)
	assert.True(t, opts.WatchDog)
}
