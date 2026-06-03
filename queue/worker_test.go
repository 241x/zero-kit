package queue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/241x/zero-kit/queue"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestQueueWithClient 创建基于 miniredis 的测试队列和客户端
func setupTestQueueWithClient(t *testing.T) (*queue.RedisQueue, *redis.Client, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	require.NoError(t, client.Ping(context.Background()).Err())

	config := queue.DefaultConfig().WithName("worker-test")
	q := queue.NewRedisQueue(client, config)

	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return q, client, cleanup
}

func TestWorker_ProcessTask(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	var processed int32
	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt32(&processed, 1)
		return nil
	})

	worker := queue.NewWorker("test-worker", q, handler)
	ctx := t.Context()

	require.NoError(t, worker.Start(ctx))

	for i := range 3 {
		task := queue.NewTask("worker-test", "test", []byte{byte(i)})
		require.NoError(t, q.Enqueue(ctx, task))
	}

	time.Sleep(2 * time.Second)
	require.NoError(t, worker.Stop())

	assert.Equal(t, int32(3), atomic.LoadInt32(&processed))
}

func TestWorker_NackOnHandlerError(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		return assert.AnError
	})

	worker := queue.NewWorker("test-worker", q, handler)
	ctx := t.Context()

	require.NoError(t, worker.Start(ctx))

	task := queue.NewTask("worker-test", "fail", []byte("fail"))
	require.NoError(t, q.Enqueue(ctx, task))

	time.Sleep(1 * time.Second)
	require.NoError(t, worker.Stop())

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Failed)
}

func TestWorker_StopsGracefully(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		return nil
	})

	worker := queue.NewWorker("test-worker", q, handler)
	ctx := t.Context()

	require.NoError(t, worker.Start(ctx))
	require.NoError(t, worker.Stop())

	task := queue.NewTask("worker-test", "test", []byte("after-stop"))
	require.NoError(t, q.Enqueue(ctx, task))

	time.Sleep(500 * time.Millisecond)

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Pending)
}

func TestWorkerPool_StartAndStop(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		return nil
	})

	config := queue.DefaultConfig().WithName("worker-test").WithMaxConcurrency(3)
	pool := queue.NewWorkerPool(q, handler, config)

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))
	assert.True(t, pool.IsRunning())

	require.NoError(t, pool.Stop())
	assert.False(t, pool.IsRunning())
}

func TestWorkerPool_ProcessesTasks(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	var processed int32
	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt32(&processed, 1)
		return nil
	})

	config := queue.DefaultConfig().WithName("worker-test").WithMaxConcurrency(3)
	pool := queue.NewWorkerPool(q, handler, config)

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))

	for i := range 10 {
		task := queue.NewTask("worker-test", "test", []byte{byte(i)})
		require.NoError(t, q.Enqueue(ctx, task))
	}

	time.Sleep(3 * time.Second)
	require.NoError(t, pool.Stop())

	assert.Equal(t, int32(10), atomic.LoadInt32(&processed))

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.Completed)
}

func TestWorkerPool_DoubleStart(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error { return nil })
	config := queue.DefaultConfig().WithName("worker-test").WithMaxConcurrency(2)
	pool := queue.NewWorkerPool(q, handler, config)

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	err := pool.Start(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
}

func TestWorkerPool_StopWhenNotRunning(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error { return nil })
	config := queue.DefaultConfig().WithName("worker-test").WithMaxConcurrency(2)
	pool := queue.NewWorkerPool(q, handler, config)

	require.NoError(t, pool.Stop())
}

func TestWorkerPool_ContextCancel(t *testing.T) {
	q, _, cleanup := setupTestQueueWithClient(t)
	defer cleanup()

	var processed int32
	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt32(&processed, 1)
		return nil
	})
	config := queue.DefaultConfig().WithName("worker-test").WithMaxConcurrency(2)
	pool := queue.NewWorkerPool(q, handler, config)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, pool.Start(ctx))

	for i := range 5 {
		task := queue.NewTask("worker-test", "test", []byte{byte(i)})
		require.NoError(t, q.Enqueue(ctx, task))
	}

	// 等待 worker 处理部分任务
	time.Sleep(2 * time.Second)

	// 取消 context 后 worker 协程退出，但仍需调用 Stop 清理状态
	cancel()
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, pool.Stop())
	assert.False(t, pool.IsRunning())

	// context 取消前已入队的任务应被处理
	assert.Greater(t, atomic.LoadInt32(&processed), int32(0))
}
