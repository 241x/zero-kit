package queue

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/241x/zero-kit/logger"
)

// Worker 从队列消费任务并交给 Handler 处理
type Worker struct {
	id      string
	queue   Queue
	handler Handler
	logger  logger.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewWorker 创建工作线程
func NewWorker(id string, queue Queue, handler Handler) *Worker {
	return &Worker{
		id:      id,
		queue:   queue,
		handler: handler,
		logger:  logger.Nop(),
	}
}

// WithLogger 设置工作线程的日志实例
func (w *Worker) WithLogger(log logger.Logger) *Worker {
	w.logger = log
	return w
}

// Start 启动工作线程
func (w *Worker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.run()
	return nil
}

// Stop 停止工作线程，等待当前任务处理完成
func (w *Worker) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	return nil
}

// run 工作线程主循环，使用自适应轮询间隔
func (w *Worker) run() {
	defer w.wg.Done()

	interval := 100 * time.Millisecond
	maxInterval := 5 * time.Second
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-timer.C:
			if w.processOne() {
				interval = 100 * time.Millisecond
			} else {
				interval = min(interval*2, maxInterval)
			}
			timer.Reset(interval)
		}
	}
}

// processOne 尝试出队并处理一个任务，返回是否成功获取任务
func (w *Worker) processOne() bool {
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()

	task, err := w.queue.Dequeue(ctx)
	if err != nil || task == nil {
		return false
	}

	if processErr := w.handler.Handle(ctx, task); processErr != nil {
		if nackErr := w.queue.Nack(ctx, task.ID, processErr); nackErr != nil {
			w.logger.Error("worker nack task failed", "worker_id", w.id, "task_id", task.ID, "error", nackErr)
		}
	} else {
		if ackErr := w.queue.Ack(ctx, task.ID); ackErr != nil {
			w.logger.Error("worker ack task failed", "worker_id", w.id, "task_id", task.ID, "error", ackErr)
		}
	}

	return true
}

// WorkerPoolImpl 工作线程池，管理多个 Worker 协程
type WorkerPoolImpl struct {
	queue   Queue
	handler Handler
	config  QueueConfig
	logger  logger.Logger
	workers []*Worker
	running bool
	mutex   sync.RWMutex
}

// NewWorkerPool 创建工作线程池
func NewWorkerPool(queue Queue, handler Handler, config QueueConfig) *WorkerPoolImpl {
	return &WorkerPoolImpl{
		queue:   queue,
		handler: handler,
		config:  config,
		logger:  logger.Nop(),
		workers: make([]*Worker, 0, config.MaxConcurrency),
	}
}

// WithLogger 设置工作线程池的日志实例
func (p *WorkerPoolImpl) WithLogger(log logger.Logger) *WorkerPoolImpl {
	p.logger = log
	return p
}

// Start 启动工作线程池，创建 config.MaxConcurrency 个工作线程
func (p *WorkerPoolImpl) Start(ctx context.Context) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.running {
		return errors.New("worker pool is already running")
	}

	for i := 0; i < p.config.MaxConcurrency; i++ {
		workerID := p.config.Name + "-worker-" + strconv.Itoa(i)
		worker := NewWorker(workerID, p.queue, p.handler)
		worker.WithLogger(p.logger)
		if err := worker.Start(ctx); err != nil {
			p.logger.Error("start worker failed", "worker_id", workerID, "pool", p.config.Name, "error", err)
			for _, w := range p.workers {
				w.Stop()
			}
			return errors.New("start worker " + strconv.Itoa(i) + " failed: " + err.Error())
		}
		p.workers = append(p.workers, worker)
	}

	p.running = true
	return nil
}

// Stop 优雅停止工作线程池，等待所有工作线程完成当前任务
func (p *WorkerPoolImpl) Stop() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.running {
		return nil
	}

	for _, worker := range p.workers {
		worker.Stop()
	}

	p.workers = p.workers[:0]
	p.running = false
	return nil
}

// IsRunning 报告线程池是否正在运行
func (p *WorkerPoolImpl) IsRunning() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.running
}
