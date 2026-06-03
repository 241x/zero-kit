package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// QueueManager 管理多个队列和工作线程池
type QueueManager struct {
	client      *redis.Client
	queues      map[string]*RedisQueue
	workerPools map[string]*WorkerPoolImpl
	mutex       sync.RWMutex
}

// NewQueueManager 创建队列管理器
func NewQueueManager(client *redis.Client) *QueueManager {
	return &QueueManager{
		client:      client,
		queues:      make(map[string]*RedisQueue),
		workerPools: make(map[string]*WorkerPoolImpl),
	}
}

// GetOrCreateQueue 获取或创建指定名称的队列
func (m *QueueManager) GetOrCreateQueue(name string, config QueueConfig) *RedisQueue {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if q, exists := m.queues[name]; exists {
		return q
	}

	config.Name = name
	q := NewRedisQueue(m.client, config)
	m.queues[name] = q
	return q
}

// RegisterWorkerPool 注册工作线程池，如果队列不存在则自动创建
func (m *QueueManager) RegisterWorkerPool(queueName string, handler Handler, config QueueConfig) (*WorkerPoolImpl, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	queue := m.getOrCreateQueueLocked(queueName, config)
	pool := NewWorkerPool(queue, handler, config)
	m.workerPools[queueName] = pool

	return pool, nil
}

// StartAllWorkerPools 启动所有工作线程池
func (m *QueueManager) StartAllWorkerPools(ctx context.Context) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	for name, pool := range m.workerPools {
		if err := pool.Start(ctx); err != nil {
			return fmt.Errorf("start worker pool %s failed: %w", name, err)
		}
	}
	return nil
}

// StopAllWorkerPools 停止所有工作线程池
func (m *QueueManager) StopAllWorkerPools() error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var errs []error
	for name, pool := range m.workerPools {
		if err := pool.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("stop worker pool %s failed: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("stop worker pools failed: %v", errs)
	}
	return nil
}

// EnqueueTask 入队任务到指定队列
func (m *QueueManager) EnqueueTask(ctx context.Context, queueName string, task *Task) error {
	queue := m.GetOrCreateQueue(queueName, DefaultConfig())
	return queue.Enqueue(ctx, task)
}

// EnqueueTaskWithDelay 延迟入队任务到指定队列
func (m *QueueManager) EnqueueTaskWithDelay(ctx context.Context, queueName string, task *Task, delay time.Duration) error {
	queue := m.GetOrCreateQueue(queueName, DefaultConfig())
	return queue.EnqueueWithDelay(ctx, task, delay)
}

// GetQueueStats 获取所有队列的统计信息
func (m *QueueManager) GetQueueStats(ctx context.Context) (map[string]*QueueStats, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	stats := make(map[string]*QueueStats, len(m.queues))
	for name, queue := range m.queues {
		queueStats, err := queue.GetStats(ctx)
		if err != nil {
			return nil, fmt.Errorf("get stats for queue %s failed: %w", name, err)
		}
		stats[name] = queueStats
	}
	return stats, nil
}

// getOrCreateQueueLocked 在已持有锁的情况下获取或创建队列
func (m *QueueManager) getOrCreateQueueLocked(name string, config QueueConfig) *RedisQueue {
	if q, exists := m.queues[name]; exists {
		return q
	}

	config.Name = name
	q := NewRedisQueue(m.client, config)
	m.queues[name] = q
	return q
}
