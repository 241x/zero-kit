package queue

import (
	"context"
	"errors"
	"time"
)

// 队列操作公共错误
var (
	ErrQueueClosed  = errors.New("queue is closed")
	ErrTaskNotFound = errors.New("task not found")
	ErrNilTask      = errors.New("task cannot be nil")
	ErrEmptyQueue   = errors.New("queue name cannot be empty")
)

// Handler 任务处理器接口
type Handler interface {
	// Handle 处理任务
	Handle(ctx context.Context, task *Task) error
}

// HandlerFunc 函数类型适配 Handler 接口
type HandlerFunc func(ctx context.Context, task *Task) error

// Handle 实现 Handler 接口
func (f HandlerFunc) Handle(ctx context.Context, task *Task) error {
	return f(ctx, task)
}

// Queue 工作队列核心接口，队列只存储任务 ID，任务详情独立存储
type Queue interface {
	// Enqueue 立即入队任务
	Enqueue(ctx context.Context, task *Task) error

	// EnqueueWithDelay 延迟入队任务
	EnqueueWithDelay(ctx context.Context, task *Task, delay time.Duration) error

	// Dequeue 出队一个可消费任务，无任务时返回 (nil, nil)
	Dequeue(ctx context.Context) (*Task, error)

	// Ack 通过任务 ID 确认处理成功
	Ack(ctx context.Context, taskID string) error

	// Nack 确认处理失败，自动重试或移入死信队列
	Nack(ctx context.Context, taskID string, taskErr error) error

	// GetTask 通过 ID 获取任务详情
	GetTask(ctx context.Context, taskID string) (*Task, error)

	// GetStats 获取队列统计信息
	GetStats(ctx context.Context) (*QueueStats, error)

	// Purge 清空队列所有数据
	Purge(ctx context.Context) error

	// Close 释放队列资源
	Close() error
}

// QueueStats 队列统计信息
type QueueStats struct {
	Name       string `json:"name"`        // 队列名称
	Pending    int64  `json:"pending"`     // 等待处理任务数
	Processing int64  `json:"processing"`  // 处理中任务数
	Delayed    int64  `json:"delayed"`     // 延迟队列任务数
	Completed  int64  `json:"completed"`   // 已完成任务总数
	Failed     int64  `json:"failed"`      // 已失败任务总数
	DeadLetter int64  `json:"dead_letter"` // 死信队列任务数
}

// WorkerPool 工作线程池接口
type WorkerPool interface {
	// Start 启动工作线程池
	Start(ctx context.Context) error

	// Stop 停止工作线程池
	Stop() error

	// IsRunning 线程池是否正在运行
	IsRunning() bool
}
