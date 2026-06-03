package locker

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis 设置测试 Redis 客户端
func setupTestRedis(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "192.168.91.100:6379",
		Password: "123456",
		DB:       1, // 使用 DB 1 进行测试
	})

	ctx := context.Background()
	err := client.Ping(ctx).Err()
	if err != nil {
		t.Skip("Redis not available, skipping test")
	}

	// 测试前清空数据库
	client.FlushDB(ctx)

	return client
}

// TestRedisLocker_LockAndUnlock 测试基本的加锁和解锁
func TestRedisLocker_LockAndUnlock(t *testing.T) {
	client := setupTestRedis(t)
	defer client.Close()

	locker := NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:basic"

	// 获取锁
	lock, err := locker.Lock(ctx, key, WithTTL(10*time.Second))
	require.NoError(t, err)
	assert.NotNil(t, lock)
	assert.Equal(t, key, lock.Key())
	assert.NotEmpty(t, lock.Token())

	// 释放锁
	err = lock.Unlock(ctx)
	require.NoError(t, err)

	// 验证锁已释放（需要检查带前缀的 key）
	exists, err := client.Exists(ctx, "lock:"+key).Result()
	assert.Equal(t, int64(0), exists)
}

// TestRedisLocker_LockConflict 测试锁冲突
func TestRedisLocker_LockConflict(t *testing.T) {
	client := setupTestRedis(t)
	defer client.Close()

	locker := NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:conflict"

	// 第一次获取锁
	lock1, err := locker.Lock(ctx, key, WithTTL(10*time.Second))
	require.NoError(t, err)

	// 第二次获取同一把锁应该失败
	lock2, err := locker.Lock(ctx, key, WithTTL(10*time.Second))
	assert.Error(t, err)
	assert.Equal(t, ErrLockAcquired, err)
	assert.Nil(t, lock2)

	// 释放第一把锁
	err = lock1.Unlock(ctx)
	require.NoError(t, err)

	// 现在可以获取锁了
	lock3, err := locker.Lock(ctx, key, WithTTL(10*time.Second))
	require.NoError(t, err)
	assert.NotNil(t, lock3)
	lock3.Unlock(ctx)
}

// TestRedisLocker_Extend 测试锁延期
func TestRedisLocker_Extend(t *testing.T) {
	client := setupTestRedis(t)
	defer client.Close()

	locker := NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:extend"

	// 获取锁，过期时间 2 秒
	lock, err := locker.Lock(ctx, key, WithTTL(2*time.Second))
	require.NoError(t, err)

	// 延期到 10 秒
	err = lock.Extend(ctx, 10*time.Second)
	require.NoError(t, err)

	// 验证过期时间已延长（需要检查带前缀的 key）
	ttl, err := client.TTL(ctx, "lock:"+key).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl.Seconds(), 5.0)

	lock.Unlock(ctx)
}

// TestRedisLocker_InvalidTokenUnlock 测试使用错误令牌解锁
func TestRedisLocker_InvalidTokenUnlock(t *testing.T) {
	client := setupTestRedis(t)
	defer client.Close()

	locker := NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:invalid_token"

	// 获取锁
	lock, err := locker.Lock(ctx, key, WithTTL(10*time.Second))
	require.NoError(t, err)

	// 创建一个新的锁对象，使用不同的 token
	wrongLock := &redisLock{
		key:    key,
		token:  "wrong_token",
		client: client,
	}

	// 尝试用错误的 token 解锁
	err = wrongLock.Unlock(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidToken, err)

	// 正确的锁应该还能解锁
	err = lock.Unlock(ctx)
	require.NoError(t, err)
}

// TestRedisLocker_ConcurrentLock 测试并发加锁
func TestRedisLocker_ConcurrentLock(t *testing.T) {
	client := setupTestRedis(t)
	defer client.Close()

	locker := NewRedisLocker(client)
	ctx := context.Background()
	key := "test:lock:concurrent"

	results := make(chan error, 10)

	// 10 个协程同时尝试获取同一把锁
	for range 10 {
		go func() {
			lock, err := locker.Lock(ctx, key, WithTTL(5*time.Second))
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
		} else if err == ErrLockAcquired {
			failCount++
		}
	}

	// 应该只有 1 个成功，其他 9 个失败
	assert.Equal(t, 1, successCount)
	assert.Equal(t, 9, failCount)
}

// TestGenerateToken 测试令牌生成
func TestGenerateToken(t *testing.T) {
	token1 := generateToken()
	token2 := generateToken()

	// 令牌长度应该是 32 字节的十六进制表示
	assert.Len(t, token1, 32)
	assert.Len(t, token2, 32)

	// 令牌应该唯一
	assert.NotEqual(t, token1, token2)
}

// TestLockOptions 测试选项函数
func TestLockOptions(t *testing.T) {
	opts := defaultLockOptions()
	assert.Equal(t, 30*time.Second, opts.TTL)
	assert.False(t, opts.WatchDog)

	// 应用自定义选项
	WithTTL(60 * time.Second)(opts)
	WithWatchDog()(opts)

	assert.Equal(t, 60*time.Second, opts.TTL)
	assert.True(t, opts.WatchDog)
}
