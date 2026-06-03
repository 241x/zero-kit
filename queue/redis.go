package queue

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lua 脚本：队列只存储任务 ID，任务详情存储在独立 String key 中，所有操作通过 Lua 脚本保证原子性。
var (
	// enqueueScript 原子入队：SET 任务数据 + LPUSH ID 到立即执行队列
	// KEYS[1]=task key, KEYS[2]=immediate queue key, ARGV[1]=task JSON, ARGV[2]=task ID
	enqueueScript = redis.NewScript(`
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('LPUSH', KEYS[2], ARGV[2])
		return 1
	`)

	// enqueueDelayedScript 原子延迟入队：SET 任务数据 + ZADD ID 到延迟队列
	// KEYS[1]=task key, KEYS[2]=delayed queue key, ARGV[1]=task JSON, ARGV[2]=score, ARGV[3]=task ID
	enqueueDelayedScript = redis.NewScript(`
		redis.call('SET', KEYS[1], ARGV[1])
		redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[3])
		return 1
	`)

	// moveDelayedScript 批量将到期延迟任务移动到立即执行队列
	// KEYS[1]=delayed queue key, KEYS[2]=immediate queue key, ARGV[1]=now timestamp, ARGV[2]=batch limit
	moveDelayedScript = redis.NewScript(`
		local ids = redis.call('ZRANGEBYSCORE', KEYS[1], 0, tonumber(ARGV[1]), 'LIMIT', 0, tonumber(ARGV[2]))
		for _, id in ipairs(ids) do
			redis.call('LPUSH', KEYS[2], id)
			redis.call('ZREM', KEYS[1], id)
		end
		return #ids
	`)

	// dequeueScript 原子出队：RPOPLPUSH ID + GET 任务数据 + SETEX 超时键
	// KEYS[1]=immediate queue key, KEYS[2]=processing queue key
	// ARGV[1]=task key prefix, ARGV[2]=timeout key prefix, ARGV[3]=visibility timeout seconds
	dequeueScript = redis.NewScript(`
		local id = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
		if id == false then
			return nil
		end
		local taskKey = ARGV[1] .. id
		local data = redis.call('GET', taskKey)
		if not data then
			redis.call('LREM', KEYS[2], 1, id)
			return nil
		end
		local timeoutKey = ARGV[2] .. id
		redis.call('SETEX', timeoutKey, tonumber(ARGV[3]), '1')
		return {id, data}
	`)

	// ackScript 原子确认：LREM ID + DEL 任务数据 + DEL 超时键 + HINCRBY 统计
	// KEYS[1]=processing key, KEYS[2]=task key, KEYS[3]=timeout key, KEYS[4]=stats key, ARGV[1]=task ID
	ackScript = redis.NewScript(`
		local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
		if removed == 0 then
			return 0
		end
		redis.call('DEL', KEYS[2])
		redis.call('DEL', KEYS[3])
		redis.call('HINCRBY', KEYS[4], 'completed', 1)
		return 1
	`)

	// nackRetryScript 原子 Nack 重试：LREM ID + SET 更新任务 + ZADD 延迟队列 + DEL 超时键
	// KEYS[1]=processing key, KEYS[2]=task key, KEYS[3]=delayed key, KEYS[4]=timeout key, KEYS[5]=stats key
	// ARGV[1]=task ID, ARGV[2]=updated task JSON, ARGV[3]=retry score
	nackRetryScript = redis.NewScript(`
		local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
		if removed == 0 then
			return 0
		end
		redis.call('SET', KEYS[2], ARGV[2])
		redis.call('ZADD', KEYS[3], tonumber(ARGV[3]), ARGV[1])
		redis.call('DEL', KEYS[4])
		redis.call('HINCRBY', KEYS[5], 'failed', 1)
		return 1
	`)

	// nackDeadLetterScript 原子 Nack 死信：LREM ID + SET 更新任务 + LPUSH 死信队列 + DEL 超时键
	// KEYS[1]=processing key, KEYS[2]=task key, KEYS[3]=dead letter key, KEYS[4]=timeout key, KEYS[5]=stats key
	// ARGV[1]=task ID, ARGV[2]=updated task JSON
	nackDeadLetterScript = redis.NewScript(`
		local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
		if removed == 0 then
			return 0
		end
		redis.call('SET', KEYS[2], ARGV[2])
		redis.call('LPUSH', KEYS[3], ARGV[1])
		redis.call('DEL', KEYS[4])
		redis.call('HINCRBY', KEYS[5], 'dead_letter', 1)
		return 1
	`)
)

// RedisQueue 基于 Redis 的工作队列，队列只存任务 ID，详情独立存储，通过 Lua 脚本保证原子性。
//
// Redis 数据结构：
//
//	{prefix}IMMEDIATE     → List   立即执行队列
//	{prefix}DELAYED       → ZSet   延迟队列（score=执行时间戳）
//	{prefix}PROCESSING    → List   处理中队列
//	{prefix}DEAD          → List   死信队列
//	{prefix}TASK:{id}     → String 任务 JSON 数据
//	{prefix}TIMEOUT:{id}  → String 可见性超时追踪（带 TTL）
//	{prefix}STATS         → Hash   统计计数
type RedisQueue struct {
	client *redis.Client
	config QueueConfig
	prefix string
}

// NewRedisQueue 创建 Redis 队列实例，Redis 客户端由调用方管理生命周期
func NewRedisQueue(client *redis.Client, config QueueConfig) *RedisQueue {
	if config.Name == "" {
		config = DefaultConfig()
	}

	return &RedisQueue{
		client: client,
		config: config,
		prefix: "ZAG:QUEUE:" + config.Name + ":",
	}
}

// Enqueue 立即入队任务
func (q *RedisQueue) Enqueue(ctx context.Context, task *Task) error {
	if task == nil {
		return ErrNilTask
	}

	task.Status = TaskStatusPending
	task.Queue = q.config.Name

	data, err := task.Marshal()
	if err != nil {
		return fmt.Errorf("marshal task failed: %w", err)
	}

	_, err = enqueueScript.Run(ctx, q.client,
		[]string{q.taskKey(task.ID), q.immediateKey()},
		string(data), task.ID,
	).Result()
	if err != nil {
		return fmt.Errorf("enqueue failed: %w", err)
	}

	return nil
}

// EnqueueWithDelay 延迟入队任务
func (q *RedisQueue) EnqueueWithDelay(ctx context.Context, task *Task, delay time.Duration) error {
	if task == nil {
		return ErrNilTask
	}

	task.Status = TaskStatusPending
	task.Queue = q.config.Name
	task.DelayUntil = time.Now().Add(delay).Unix()

	data, err := task.Marshal()
	if err != nil {
		return fmt.Errorf("marshal task failed: %w", err)
	}

	_, err = enqueueDelayedScript.Run(ctx, q.client,
		[]string{q.taskKey(task.ID), q.delayedKey()},
		string(data), task.DelayUntil, task.ID,
	).Result()
	if err != nil {
		return fmt.Errorf("enqueue delayed failed: %w", err)
	}

	return nil
}

// Dequeue 出队一个可消费任务，无任务时返回 (nil, nil)
func (q *RedisQueue) Dequeue(ctx context.Context) (*Task, error) {
	// 将到期的延迟任务移动到立即执行队列
	if err := q.moveDelayedTasks(ctx); err != nil {
		return nil, fmt.Errorf("move delayed tasks failed: %w", err)
	}

	// 原子出队：RPOPLPUSH + GET 任务数据 + SETEX 超时键
	result, err := dequeueScript.Run(ctx, q.client,
		[]string{q.immediateKey(), q.processingKey()},
		q.prefix+"TASK:", q.prefix+"TIMEOUT:", q.config.VisibilityTimeout,
	).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("dequeue failed: %w", err)
	}
	if result == nil {
		return nil, nil
	}

	slice, ok := result.([]any)
	if !ok || len(slice) < 2 {
		return nil, nil
	}

	taskID, _ := slice[0].(string)
	taskData, _ := slice[1].(string)

	task, err := UnmarshalTask([]byte(taskData))
	if err != nil {
		// 数据损坏，清理孤儿 ID
		q.client.LRem(ctx, q.processingKey(), 1, taskID)
		q.client.Del(ctx, q.taskKey(taskID))
		return nil, fmt.Errorf("unmarshal task %s failed: %w", taskID, err)
	}

	// 更新任务状态为处理中
	task.Status = TaskStatusProcessing
	task.StartedAt = time.Now().Unix()

	updatedData, err := task.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal updated task failed: %w", err)
	}

	if err := q.client.Set(ctx, q.taskKey(taskID), updatedData, 0).Err(); err != nil {
		return nil, fmt.Errorf("update task status failed: %w", err)
	}

	return task, nil
}

// Ack 通过任务 ID 确认处理成功
func (q *RedisQueue) Ack(ctx context.Context, taskID string) error {
	result, err := ackScript.Run(ctx, q.client,
		[]string{q.processingKey(), q.taskKey(taskID), q.timeoutKey(taskID), q.statsKey()},
		taskID,
	).Int64()
	if err != nil {
		return fmt.Errorf("ack failed: %w", err)
	}
	if result == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// Nack 确认处理失败，未超重试上限则延迟重试，否则移入死信队列
func (q *RedisQueue) Nack(ctx context.Context, taskID string, taskErr error) error {
	taskData, err := q.client.Get(ctx, q.taskKey(taskID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrTaskNotFound
		}
		return fmt.Errorf("get task failed: %w", err)
	}

	task, err := UnmarshalTask(taskData)
	if err != nil {
		return fmt.Errorf("unmarshal task failed: %w", err)
	}

	task.RetryCount++
	task.Error = ""
	if taskErr != nil {
		task.Error = taskErr.Error()
	}

	// 超过最大重试次数 → 移入死信队列
	if task.RetryCount > task.MaxRetries {
		task.Status = TaskStatusDeadLetter
		task.CompletedAt = time.Now().Unix()

		updatedData, marshalErr := task.Marshal()
		if marshalErr != nil {
			return fmt.Errorf("marshal task failed: %w", marshalErr)
		}

		result, scriptErr := nackDeadLetterScript.Run(ctx, q.client,
			[]string{q.processingKey(), q.taskKey(taskID), q.deadLetterKey(), q.timeoutKey(taskID), q.statsKey()},
			taskID, string(updatedData),
		).Int64()
		if scriptErr != nil {
			return fmt.Errorf("nack to dead letter failed: %w", scriptErr)
		}
		if result == 0 {
			return ErrTaskNotFound
		}
		return nil
	}

	// 未超过重试上限 → 延迟重试
	task.Status = TaskStatusPending
	delay := q.calculateRetryDelay(task.RetryCount)
	task.DelayUntil = time.Now().Add(delay).Unix()

	updatedData, marshalErr := task.Marshal()
	if marshalErr != nil {
		return fmt.Errorf("marshal task failed: %w", marshalErr)
	}

	result, scriptErr := nackRetryScript.Run(ctx, q.client,
		[]string{q.processingKey(), q.taskKey(taskID), q.delayedKey(), q.timeoutKey(taskID), q.statsKey()},
		taskID, string(updatedData), task.DelayUntil,
	).Int64()
	if scriptErr != nil {
		return fmt.Errorf("nack retry failed: %w", scriptErr)
	}
	if result == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// GetTask 通过 ID 获取任务详情
func (q *RedisQueue) GetTask(ctx context.Context, taskID string) (*Task, error) {
	data, err := q.client.Get(ctx, q.taskKey(taskID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrTaskNotFound
		}
		return nil, fmt.Errorf("get task failed: %w", err)
	}

	task, err := UnmarshalTask(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal task failed: %w", err)
	}

	return task, nil
}

// GetStats 获取队列统计信息
func (q *RedisQueue) GetStats(ctx context.Context) (*QueueStats, error) {
	stats := &QueueStats{Name: q.config.Name}

	pipe := q.client.Pipeline()
	pendingCmd := pipe.LLen(ctx, q.immediateKey())
	delayedCmd := pipe.ZCard(ctx, q.delayedKey())
	processingCmd := pipe.LLen(ctx, q.processingKey())
	deadLetterCmd := pipe.LLen(ctx, q.deadLetterKey())
	statsCmd := pipe.HGetAll(ctx, q.statsKey())

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("get stats failed: %w", err)
	}

	stats.Pending = pendingCmd.Val()
	stats.Delayed = delayedCmd.Val()
	stats.Processing = processingCmd.Val()
	stats.DeadLetter = deadLetterCmd.Val()

	statsMap := statsCmd.Val()
	if v, ok := statsMap["completed"]; ok {
		stats.Completed, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := statsMap["failed"]; ok {
		stats.Failed, _ = strconv.ParseInt(v, 10, 64)
	}

	return stats, nil
}

// Purge 清空队列所有数据
func (q *RedisQueue) Purge(ctx context.Context) error {
	mainKeys := []string{
		q.immediateKey(),
		q.delayedKey(),
		q.processingKey(),
		q.deadLetterKey(),
		q.statsKey(),
	}
	for _, key := range mainKeys {
		if err := q.client.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("delete key %s failed: %w", key, err)
		}
	}

	if err := q.deleteKeysByPattern(ctx, q.prefix+"TASK:*"); err != nil {
		return fmt.Errorf("delete task keys failed: %w", err)
	}

	if err := q.deleteKeysByPattern(ctx, q.prefix+"TIMEOUT:*"); err != nil {
		return fmt.Errorf("delete timeout keys failed: %w", err)
	}

	return nil
}

// Close 释放队列资源（Redis 客户端由外部管理）
func (q *RedisQueue) Close() error {
	return nil
}

// moveDelayedTasks 将到期的延迟任务批量移动到立即执行队列，每次最多 100 个
func (q *RedisQueue) moveDelayedTasks(ctx context.Context) error {
	now := time.Now().Unix()
	_, err := moveDelayedScript.Run(ctx, q.client,
		[]string{q.delayedKey(), q.immediateKey()},
		now, 100,
	).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

// calculateRetryDelay 根据重试策略计算重试延迟
func (q *RedisQueue) calculateRetryDelay(retryCount int) time.Duration {
	switch q.config.RetryDelay {
	case RetryDelayFixed:
		return 5 * time.Second
	case RetryDelayExponential:
		delay := time.Duration(1<<uint(retryCount)) * time.Second
		if delay > time.Hour {
			return time.Hour
		}
		return delay
	case RetryDelayRandom:
		return time.Duration(1+rand.Intn(10)) * time.Second
	default:
		return 5 * time.Second
	}
}

// deleteKeysByPattern 扫描匹配模式并批量删除键
func (q *RedisQueue) deleteKeysByPattern(ctx context.Context, pattern string) error {
	iter := q.client.Scan(ctx, 0, pattern, 100).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return q.client.Del(ctx, keys...).Err()
	}
	return nil
}

// immediateKey 获取立即执行队列的键名
func (q *RedisQueue) immediateKey() string {
	return q.prefix + "IMMEDIATE"
}

// delayedKey 获取延迟队列的键名
func (q *RedisQueue) delayedKey() string {
	return q.prefix + "DELAYED"
}

// processingKey 获取处理中的队列的键名
func (q *RedisQueue) processingKey() string {
	return q.prefix + "PROCESSING"
}

// deadLetterKey 获取死信队列的键名
func (q *RedisQueue) deadLetterKey() string {
	return q.prefix + "DEAD"
}

// taskKey 获取任务详情的键名
func (q *RedisQueue) taskKey(id string) string {
	return q.prefix + "TASK:" + id
}

// timeoutKey 获取任务超时的键名
func (q *RedisQueue) timeoutKey(id string) string {
	return q.prefix + "TIMEOUT:" + id
}

// statsKey 获取队列统计信息的键名
func (q *RedisQueue) statsKey() string {
	return q.prefix + "STATS"
}
