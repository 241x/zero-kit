package job

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/241x/zero-kit/logger"
)

// ErrExecutorRunning 执行器已启动
var ErrExecutorRunning = errors.New("job: executor is already running")

// ErrStopTimeout 执行器停止超时，仍有处理器未退出
var ErrStopTimeout = errors.New("job: executor stop timed out")

// Executor 消费 pending 作业并执行，负责心跳保活、进度上报、失败重试与失联恢复。
type Executor struct {
	store   Store
	handler Handler
	config  Config
	logger  logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	running bool
	mu      sync.Mutex
}

// NewExecutor 创建作业执行器
func NewExecutor(store Store, handler Handler, config Config) *Executor {
	return &Executor{
		store:   store,
		handler: handler,
		config:  config,
		logger:  logger.Nop(),
	}
}

// WithLogger 设置执行器的日志实例
func (e *Executor) WithLogger(log logger.Logger) *Executor {
	e.logger = log
	return e
}

// Start 启动执行器，为每个队列创建 config.Concurrency 个执行协程及维护协程
func (e *Executor) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return ErrExecutorRunning
	}
	if e.store == nil {
		return errors.New("job: store cannot be nil")
	}
	if e.handler == nil {
		return errors.New("job: handler cannot be nil")
	}
	if len(e.config.Queues) == 0 {
		e.config.Queues = []string{DefaultQueue}
	}
	if err := e.config.Validate(); err != nil {
		return err
	}

	e.ctx, e.cancel = context.WithCancel(ctx)

	for _, queue := range e.config.Queues {
		for i := 0; i < e.config.Concurrency; i++ {
			e.wg.Add(1)
			go e.worker(queue)
		}
	}

	e.wg.Add(1)
	go e.maintain()

	e.running = true
	return nil
}

// Stop 优雅停止执行器，取消正在执行的作业并等待退出。
// 若处理器未在 ShutdownTimeout 内响应取消，则强制返回 ErrStopTimeout，
// 残留的 running 作业将由下次启动的失联恢复接管。
func (e *Executor) Stop() error {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return nil
	}
	e.cancel()
	e.mu.Unlock()

	// 不在持锁状态下等待，避免阻塞 IsRunning/Start
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	timeout := e.config.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		e.logger.Warn("executor stop timed out, some handlers still running")
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
		return ErrStopTimeout
	}

	e.mu.Lock()
	e.running = false
	e.mu.Unlock()
	return nil
}

// IsRunning 报告执行器是否正在运行
func (e *Executor) IsRunning() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.running
}

// worker 执行协程：循环取出指定队列的 pending 作业并执行
func (e *Executor) worker(queue string) {
	defer e.wg.Done()

	const idleInterval = 200 * time.Millisecond

	for {
		select {
		case <-e.ctx.Done():
			return
		default:
		}

		job, err := e.store.PopPending(e.ctx, queue)
		if err != nil {
			e.logger.Error("pop pending job failed", "queue", queue, "error", err)
			e.sleep(idleInterval)
			continue
		}
		if job == nil {
			e.sleep(idleInterval)
			continue
		}

		e.execute(job)
	}
}

// execute 执行单个作业，维护心跳并写入终态或重试
func (e *Executor) execute(job *Job) {
	log := e.logger.With("job_id", job.ID, "job_type", job.Type, "queue", job.Queue)
	log.Info("job started", "attempt", job.Attempts)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	if e.config.JobTimeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, e.config.JobTimeout)
		defer timeoutCancel()
	}

	// 进度上报：handler 通过 ReportProgress 直接落库
	ctx = withProgress(ctx, func(p int) {
		if _, err := e.store.Heartbeat(e.ctx, job.Queue, job.ID, job.Attempts, p); err != nil {
			log.Warn("report progress failed", "error", err)
		}
	})

	// 心跳协程：周期保活，并在作业被取消时终止执行。
	// 当作业上下文被取消（超时或执行器停止）时停止心跳，
	// 使卡死的处理器不再被保活，作业可被失联恢复接管。
	heartbeatDone := make(chan struct{})
	var heartbeatWg sync.WaitGroup
	heartbeatWg.Go(func() {
		ticker := time.NewTicker(e.config.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				running, err := e.store.Heartbeat(e.ctx, job.Queue, job.ID, job.Attempts, -1)
				if err != nil {
					log.Warn("job heartbeat failed", "error", err)
					continue
				}
				if !running {
					// 作业已被取消或状态异常，终止执行
					log.Warn("job is no longer running, aborting")
					cancel()
					return
				}
			}
		}
	})

	// 执行作业
	err := e.runHandler(ctx, job, log)
	close(heartbeatDone)
	heartbeatWg.Wait()

	// 写入终态（使用执行器 ctx，独立于 handler 超时，同时保证 Stop 可快速返回）
	if err == nil {
		log.Info("job completed")
		if completeErr := e.store.Complete(e.ctx, job.Queue, job.ID, job.Attempts, job.Result); completeErr != nil {
			log.Error("mark job completed error", "error", completeErr)
		}
		return
	}

	// 记录失败信息
	failure := Failure{
		Error: err.Error(),
		Errors: appendError(job.Errors, AttemptError{
			Attempt: job.Attempts,
			At:      time.Now().UnixMilli(),
			Error:   err.Error(),
		}),
	}

	// 超过最大执行次数则最终失败，否则进入待重试
	if job.Attempts >= job.MaxAttempts {
		log.Err(err, "job failed permanently", "attempts", job.Attempts)
		if failErr := e.store.Fail(e.ctx, job.Queue, job.ID, job.Attempts, failure); failErr != nil {
			log.Error("mark job failed error", "error", failErr)
		}
		return
	}

	retryAt := time.Now().Add(e.retryDelayJittered(job.Attempts))
	log.Warn("job failed, scheduling retry", "attempts", job.Attempts, "scheduled_at", retryAt)
	if retryErr := e.store.Retry(e.ctx, job.Queue, job.ID, job.Attempts, retryAt, failure); retryErr != nil {
		log.Error("schedule job retry error", "error", retryErr)
	}
}

// runHandler 执行作业处理器，捕获 panic 避免拖垮整个进程
func (e *Executor) runHandler(ctx context.Context, job *Job, log logger.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, trimStack(debug.Stack()))
			log.Error("handler panicked", "panic", r)
		}
	}()
	return e.handler.Execute(ctx, job)
}

// maintain 维护协程：周期执行失联恢复与终态清理
func (e *Executor) maintain() {
	defer e.wg.Done()

	recoverTicker := time.NewTicker(e.config.RecoverInterval)
	defer recoverTicker.Stop()

	// Retention 为 0 时关闭清理：cleanupC 保持 nil，永不触发
	var cleanupTicker *time.Ticker
	var cleanupC <-chan time.Time
	if e.config.Retention > 0 {
		cleanupTicker = time.NewTicker(e.config.CleanupInterval)
		defer cleanupTicker.Stop()
		cleanupC = cleanupTicker.C
	}

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-recoverTicker.C:
			for _, queue := range e.config.Queues {
				e.recoverStale(queue)
			}
		case <-cleanupC:
			n, err := e.store.Cleanup(e.ctx, e.config.Retention)
			if err != nil {
				e.logger.Error("cleanup finished jobs failed", "error", err)
			} else if n > 0 {
				e.logger.Info("cleaned up finished jobs", "count", n)
			}
		}
	}
}

// recoverStale 扫描并恢复指定队列的失联作业
func (e *Executor) recoverStale(queue string) {
	since := time.Now().Add(-e.config.StaleTimeout)
	jobs, err := e.store.ListStaleRunning(e.ctx, queue, since, 100)
	if err != nil {
		e.logger.Error("list stale running jobs failed", "queue", queue, "error", err)
		return
	}

	for _, job := range jobs {
		log := e.logger.With("job_id", job.ID, "job_type", job.Type, "queue", queue)

		// 超过最大执行次数则标记失败，否则重新入队
		if job.MaxAttempts > 0 && job.Attempts >= job.MaxAttempts {
			log.Warn("stale job exceeded max attempts, marking failed", "attempts", job.Attempts)
			failure := Failure{
				Error: "job exceeded max attempts",
				Errors: appendError(job.Errors, AttemptError{
					Attempt: job.Attempts,
					At:      time.Now().UnixMilli(),
					Error:   "job exceeded max attempts",
				}),
			}
			if failErr := e.store.Fail(e.ctx, queue, job.ID, job.Attempts, failure); failErr != nil {
				log.Error("mark stale job failed error", "error", failErr)
			}
			continue
		}

		log.Warn("job heartbeat stale, requeueing", "attempts", job.Attempts)
		if requeueErr := e.store.Requeue(e.ctx, queue, job.ID); requeueErr != nil {
			log.Error("requeue stale job failed", "error", requeueErr)
		}
	}
}

// retryDelay 根据重试策略计算退避延迟
func (e *Executor) retryDelay(attempt int) time.Duration {
	if e.config.RetryStrategy == RetryStrategyFixed {
		return e.config.RetryDelay
	}

	base := e.config.RetryDelay
	if base <= 0 {
		return 0
	}

	// 指数退避：迭代翻倍，受 RetryMaxDelay 封顶；
	// RetryMaxDelay 为 0 时视为无上限，仅做溢出保护。
	max := e.config.RetryMaxDelay
	delay := base
	for i := 1; i < attempt; i++ {
		if max > 0 && delay >= max/2 {
			return max
		}
		if delay >= time.Duration(1<<62) {
			return delay
		}
		delay *= 2
	}
	if max > 0 && delay > max {
		return max
	}
	return delay
}

// retryDelayJittered 在基础退避延迟上叠加随机抖动（full jitter，范围为 [0, delay)），
// 用于打散批量失败任务的重试时间，避免惊群；RetryJitter 关闭或延迟为 0 时返回原值。
func (e *Executor) retryDelayJittered(attempt int) time.Duration {
	delay := e.retryDelay(attempt)
	if !e.config.RetryJitter || delay <= 0 {
		return delay
	}
	return time.Duration(rand.Int64N(int64(delay)))
}

// sleep 在可取消情况下等待指定时长
func (e *Executor) sleep(d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-e.ctx.Done():
	case <-timer.C:
	}
}

// appendError 追加错误到历史并返回新切片
func appendError(errs []AttemptError, e AttemptError) []AttemptError {
	out := make([]AttemptError, 0, len(errs)+1)
	out = append(out, errs...)
	out = append(out, e)
	return out
}

// maxPanicStackLen panic 堆栈入库的最大字节数，避免错误信息过大或泄露过多上下文
const maxPanicStackLen = 4096

// trimStack 将堆栈信息截断到 maxPanicStackLen，超出部分以省略标记结尾
func trimStack(stack []byte) string {
	if len(stack) > maxPanicStackLen {
		return string(stack[:maxPanicStackLen]) + "\n...(truncated)"
	}
	return string(stack)
}
