package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/241x/zero-kit/queue"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestQueue 创建基于 miniredis 的测试队列，返回队列实例和清理函数。
func setupTestQueue(t *testing.T, config queue.QueueConfig) (*queue.RedisQueue, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	require.NoError(t, client.Ping(context.Background()).Err())

	q := queue.NewRedisQueue(client, config)

	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return q, cleanup
}

// defaultTestConfig 返回测试用默认配置
func defaultTestConfig() queue.QueueConfig {
	return queue.DefaultConfig().WithName("test-queue")
}

// ---------- Enqueue 测试 ----------

func TestRedisQueue_Enqueue_Basic(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("test-queue", "email", []byte("hello"))

	err := q.Enqueue(ctx, task)
	require.NoError(t, err)

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Pending)
}

func TestRedisQueue_Enqueue_NilTask(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	err := q.Enqueue(context.Background(), nil)
	assert.ErrorIs(t, err, queue.ErrNilTask)
}

func TestRedisQueue_Enqueue_SetsTaskFields(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("original-queue", "test-type", []byte("payload"))

	err := q.Enqueue(ctx, task)
	require.NoError(t, err)

	saved, err := q.GetTask(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, queue.TaskStatusPending, saved.Status)
	assert.Equal(t, "test-queue", saved.Queue)
	assert.Equal(t, "test-type", saved.Type)
	assert.Equal(t, []byte("payload"), saved.Payload)
}

func TestRedisQueue_Enqueue_MultipleTasks(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()

	for i := range 10 {
		task := queue.NewTask("test-queue", "batch", []byte{byte(i)})
		err := q.Enqueue(ctx, task)
		require.NoError(t, err)
	}

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.Pending)
}

// ---------- Dequeue 测试 ----------

func TestRedisQueue_Dequeue_Basic(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("test-queue", "email", []byte("hello"))
	require.NoError(t, q.Enqueue(ctx, task))

	dequeued, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	assert.Equal(t, task.ID, dequeued.ID)
	assert.Equal(t, queue.TaskStatusProcessing, dequeued.Status)
	assert.NotEqual(t, int64(0), dequeued.StartedAt)

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.Pending)
	assert.Equal(t, int64(1), stats.Processing)
}

func TestRedisQueue_Dequeue_EmptyQueue(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	task, err := q.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, task)
}

func TestRedisQueue_Dequeue_FIFO(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()

	task1 := queue.NewTask("test-queue", "first", []byte("1"))
	task2 := queue.NewTask("test-queue", "second", []byte("2"))
	task3 := queue.NewTask("test-queue", "third", []byte("3"))

	require.NoError(t, q.Enqueue(ctx, task1))
	require.NoError(t, q.Enqueue(ctx, task2))
	require.NoError(t, q.Enqueue(ctx, task3))

	first, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, task1.ID, first.ID)

	second, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, task2.ID, second.ID)

	third, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, task3.ID, third.ID)
}

// ---------- Ack 测试 ----------

func TestRedisQueue_Ack_Basic(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("test-queue", "email", []byte("hello"))
	require.NoError(t, q.Enqueue(ctx, task))

	dequeued, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	err = q.Ack(ctx, dequeued.ID)
	require.NoError(t, err)

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.Processing)
	assert.Equal(t, int64(1), stats.Completed)

	_, err = q.GetTask(ctx, dequeued.ID)
	assert.ErrorIs(t, err, queue.ErrTaskNotFound)
}

func TestRedisQueue_Ack_TaskNotFound(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	err := q.Ack(context.Background(), "non-existent-id")
	assert.ErrorIs(t, err, queue.ErrTaskNotFound)
}

// ---------- Nack 测试 ----------

func TestRedisQueue_Nack_Retry(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("test-queue", "retry", []byte("retry-me"))
	require.NoError(t, q.Enqueue(ctx, task))

	dequeued, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	err = q.Nack(ctx, dequeued.ID, assert.AnError)
	require.NoError(t, err)

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.Processing)
	assert.Equal(t, int64(1), stats.Delayed)
	assert.Equal(t, int64(1), stats.Failed)
}

func TestRedisQueue_Nack_DeadLetter(t *testing.T) {
	config := defaultTestConfig().WithMaxRetries(1)
	q, cleanup := setupTestQueue(t, config)
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("test-queue", "fail", []byte("will-fail"))
	task.WithMaxRetries(1)
	require.NoError(t, q.Enqueue(ctx, task))

	// 第一次出队 + Nack（retryCount=1，等于 MaxRetries，还未超过）
	dequeued, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	require.NoError(t, q.Nack(ctx, dequeued.ID, assert.AnError))

	// 等待延迟到期（指数退避：第 1 次重试延迟 2 秒）
	time.Sleep(3 * time.Second)

	// 第二次出队 + Nack（retryCount=2，超过 MaxRetries=1，进入死信）
	dequeued2, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued2)
	require.NoError(t, q.Nack(ctx, dequeued2.ID, assert.AnError))

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.DeadLetter)
}

func TestRedisQueue_Nack_TaskNotFound(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	err := q.Nack(context.Background(), "non-existent-id", assert.AnError)
	assert.ErrorIs(t, err, queue.ErrTaskNotFound)
}

// ---------- 延迟任务测试 ----------

func TestRedisQueue_DelayedTask_NotReady(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("test-queue", "delayed", []byte("wait"))
	require.NoError(t, q.EnqueueWithDelay(ctx, task, 10*time.Second))

	dequeued, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Nil(t, dequeued)

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Delayed)
}

func TestRedisQueue_DelayedTask_Ready(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	config := defaultTestConfig()
	q := queue.NewRedisQueue(client, config)

	ctx := context.Background()
	task := queue.NewTask("test-queue", "delayed", []byte("wait"))
	require.NoError(t, q.EnqueueWithDelay(ctx, task, 2*time.Second))

	// 立即出队应返回 nil
	dequeued, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Nil(t, dequeued)

	// 通过 go-redis 客户端直接修改 ZSet score 为过去时间，模拟延迟到期
	// 注意：miniredis.FastForward 只影响 Redis 内部 TTL，不影响 Go 的 time.Now()
	delayedKey := "ZAG:QUEUE:test-queue:DELAYED"
	pastScore := float64(time.Now().Unix() - 10)
	require.NoError(t, client.ZAdd(ctx, delayedKey, redis.Z{Score: pastScore, Member: task.ID}).Err())

	// 现在应该可以出队
	dequeued, err = q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, task.ID, dequeued.ID)
}

// ---------- GetTask 测试 ----------

func TestRedisQueue_GetTask_Existing(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()
	task := queue.NewTask("test-queue", "test", []byte("data"))
	task.WithMetadata("key", "value")
	require.NoError(t, q.Enqueue(ctx, task))

	saved, err := q.GetTask(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, saved.ID)
	assert.Equal(t, "test-queue", saved.Queue)
	assert.Equal(t, "test", saved.Type)
	assert.Equal(t, []byte("data"), saved.Payload)
	assert.Equal(t, "value", saved.Metadata["key"])
}

func TestRedisQueue_GetTask_NotFound(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	_, err := q.GetTask(context.Background(), "non-existent-id")
	assert.ErrorIs(t, err, queue.ErrTaskNotFound)
}

// ---------- GetStats 测试 ----------

func TestRedisQueue_GetStats_InitialState(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	stats, err := q.GetStats(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "test-queue", stats.Name)
	assert.Equal(t, int64(0), stats.Pending)
	assert.Equal(t, int64(0), stats.Processing)
	assert.Equal(t, int64(0), stats.Delayed)
	assert.Equal(t, int64(0), stats.Completed)
	assert.Equal(t, int64(0), stats.Failed)
	assert.Equal(t, int64(0), stats.DeadLetter)
}

func TestRedisQueue_GetStats_AfterOperations(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()

	for i := range 3 {
		task := queue.NewTask("test-queue", "test", []byte{byte(i)})
		require.NoError(t, q.Enqueue(ctx, task))
	}

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats.Pending)

	dequeued, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Ack(ctx, dequeued.ID))

	stats, err = q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.Pending)
	assert.Equal(t, int64(0), stats.Processing)
	assert.Equal(t, int64(1), stats.Completed)
}

// ---------- Purge 测试 ----------

func TestRedisQueue_Purge(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()

	for i := range 5 {
		task := queue.NewTask("test-queue", "test", []byte{byte(i)})
		require.NoError(t, q.Enqueue(ctx, task))
	}

	_, err := q.Dequeue(ctx)
	require.NoError(t, err)
	_, err = q.Dequeue(ctx)
	require.NoError(t, err)

	require.NoError(t, q.Purge(ctx))

	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.Pending)
	assert.Equal(t, int64(0), stats.Processing)
}

// ---------- Close 测试 ----------

func TestRedisQueue_Close(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	err := q.Close()
	assert.NoError(t, err)
}

// ---------- 完整工作流测试 ----------

func TestRedisQueue_FullWorkflow(t *testing.T) {
	q, cleanup := setupTestQueue(t, defaultTestConfig())
	defer cleanup()

	ctx := context.Background()

	// 1. 入队 5 个任务
	tasks := make([]*queue.Task, 5)
	for i := range 5 {
		tasks[i] = queue.NewTask("test-queue", "workflow", []byte{byte(i)})
		tasks[i].WithMetadata("index", string(rune('0'+i)))
		require.NoError(t, q.Enqueue(ctx, tasks[i]))
	}

	// 2. 逐个出队并 Ack 前 3 个
	for range 3 {
		dequeued, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.NotNil(t, dequeued)
		require.NoError(t, q.Ack(ctx, dequeued.ID))
	}

	// 3. 出队第 4 个并 Nack
	dequeued4, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued4)
	require.NoError(t, q.Nack(ctx, dequeued4.ID, assert.AnError))

	// 4. 出队第 5 个并 Ack
	dequeued5, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, dequeued5)
	require.NoError(t, q.Ack(ctx, dequeued5.ID))

	// 5. 验证最终统计
	stats, err := q.GetStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.Pending)
	assert.Equal(t, int64(0), stats.Processing)
	assert.Equal(t, int64(4), stats.Completed)
	assert.Equal(t, int64(1), stats.Failed)
	assert.Equal(t, int64(1), stats.Delayed)
}

// ---------- ID 引用设计验证 ----------

func TestRedisQueue_IDReferenceDesign(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	config := defaultTestConfig()
	q := queue.NewRedisQueue(client, config)
	ctx := context.Background()

	task := queue.NewTask("test-queue", "id-test", []byte("verify-id-design"))
	require.NoError(t, q.Enqueue(ctx, task))

	// 验证立即执行队列中存储的是任务 ID（不是完整 JSON）
	immediateKey := "ZAG:QUEUE:test-queue:IMMEDIATE"
	listVals, err := client.LRange(ctx, immediateKey, 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, listVals, 1)
	assert.Equal(t, task.ID, listVals[0], "队列中应存储任务 ID，而非完整 JSON")

	// 验证任务数据独立存储在 TASK:{id} key 中
	taskKey := "ZAG:QUEUE:test-queue:TASK:" + task.ID
	exists, err := client.Exists(ctx, taskKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "任务数据应独立存储在 TASK:{id} key 中")

	// 验证任务数据可被正确读取
	saved, err := q.GetTask(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, task.ID, saved.ID)
	assert.Equal(t, []byte("verify-id-design"), saved.Payload)
}
