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

func setupTestManager(t *testing.T) (*queue.QueueManager, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	require.NoError(t, client.Ping(context.Background()).Err())

	manager := queue.NewQueueManager(client)

	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return manager, cleanup
}

func TestQueueManager_GetOrCreateQueue(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	config := queue.DefaultConfig()

	q1 := manager.GetOrCreateQueue("test-queue", config)
	require.NotNil(t, q1)

	q2 := manager.GetOrCreateQueue("test-queue", config)
	assert.Same(t, q1, q2)

	q3 := manager.GetOrCreateQueue("other-queue", config)
	assert.NotSame(t, q1, q3)
}

func TestQueueManager_EnqueueTask(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("email", "send", []byte("hello"))

	require.NoError(t, manager.EnqueueTask(ctx, "email-queue", task))

	stats, err := manager.GetQueueStats(ctx)
	require.NoError(t, err)

	emailStats, exists := stats["email-queue"]
	require.True(t, exists)
	assert.Equal(t, int64(1), emailStats.Pending)
}

func TestQueueManager_EnqueueTaskWithDelay(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("delayed", "process", []byte("delayed"))

	require.NoError(t, manager.EnqueueTaskWithDelay(ctx, "delayed-queue", task, 5*time.Second))

	stats, err := manager.GetQueueStats(ctx)
	require.NoError(t, err)

	delayedStats, exists := stats["delayed-queue"]
	require.True(t, exists)
	assert.Equal(t, int64(1), delayedStats.Delayed)
}

func TestQueueManager_GetQueueStats(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	for i := range 3 {
		task := queue.NewTask("queue-a", "test", []byte{byte(i)})
		require.NoError(t, manager.EnqueueTask(ctx, "queue-a", task))
	}

	task := queue.NewTask("queue-b", "test", []byte("b"))
	require.NoError(t, manager.EnqueueTask(ctx, "queue-b", task))

	stats, err := manager.GetQueueStats(ctx)
	require.NoError(t, err)

	assert.Contains(t, stats, "queue-a")
	assert.Contains(t, stats, "queue-b")
	assert.Equal(t, int64(3), stats["queue-a"].Pending)
	assert.Equal(t, int64(1), stats["queue-b"].Pending)
}

func TestQueueManager_RegisterWorkerPool(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	var processed int32
	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt32(&processed, 1)
		return nil
	})

	config := queue.DefaultConfig().WithMaxConcurrency(2)
	pool, err := manager.RegisterWorkerPool("worker-queue", handler, config)
	require.NoError(t, err)
	require.NotNil(t, pool)

	require.NoError(t, manager.StartAllWorkerPools(ctx))

	for i := range 5 {
		task := queue.NewTask("worker-queue", "test", []byte{byte(i)})
		require.NoError(t, manager.EnqueueTask(ctx, "worker-queue", task))
	}

	time.Sleep(2 * time.Second)
	require.NoError(t, manager.StopAllWorkerPools())

	assert.Equal(t, int32(5), atomic.LoadInt32(&processed))
}

func TestQueueManager_StartAllWorkerPools(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	handler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error { return nil })
	config := queue.DefaultConfig().WithMaxConcurrency(1)

	_, err := manager.RegisterWorkerPool("pool-a", handler, config)
	require.NoError(t, err)
	_, err = manager.RegisterWorkerPool("pool-b", handler, config)
	require.NoError(t, err)

	require.NoError(t, manager.StartAllWorkerPools(ctx))
	require.NoError(t, manager.StopAllWorkerPools())
}

func TestQueueManager_StopAllWorkerPools_WhenNotStarted(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	require.NoError(t, manager.StopAllWorkerPools())
}

func TestQueueManager_FullWorkflow(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	var emailProcessed int32
	var notifyProcessed int32

	emailHandler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt32(&emailProcessed, 1)
		return nil
	})
	notifyHandler := queue.HandlerFunc(func(ctx context.Context, task *queue.Task) error {
		atomic.AddInt32(&notifyProcessed, 1)
		return nil
	})

	config := queue.DefaultConfig().WithMaxConcurrency(2)
	_, err := manager.RegisterWorkerPool("emails", emailHandler, config.WithName("emails"))
	require.NoError(t, err)
	_, err = manager.RegisterWorkerPool("notifications", notifyHandler, config.WithName("notifications"))
	require.NoError(t, err)

	require.NoError(t, manager.StartAllWorkerPools(ctx))

	for i := range 5 {
		emailTask := queue.NewTask("emails", "send-email", []byte{byte(i)})
		require.NoError(t, manager.EnqueueTask(ctx, "emails", emailTask))

		notifyTask := queue.NewTask("notifications", "send-notify", []byte{byte(i)})
		require.NoError(t, manager.EnqueueTask(ctx, "notifications", notifyTask))
	}

	time.Sleep(3 * time.Second)
	require.NoError(t, manager.StopAllWorkerPools())

	assert.Equal(t, int32(5), atomic.LoadInt32(&emailProcessed))
	assert.Equal(t, int32(5), atomic.LoadInt32(&notifyProcessed))

	stats, err := manager.GetQueueStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), stats["emails"].Completed)
	assert.Equal(t, int64(5), stats["notifications"].Completed)
}
