package job

import (
	"context"
	"time"
)

// Store 作业存储接口，负责作业状态的持久化与原子状态转移。
//
// 实现必须保证状态转移（如 PopPending、Complete、Fail、Retry）的原子性，
// 以避免并发执行器重复消费同一作业。
//
// 作业详情按全局唯一 ID 存储，队列索引按 queue 维度隔离。
type Store interface {
	// Save 保存（创建）作业，按 job.Queue 与 job.ScheduledAt 决定入队位置
	Save(ctx context.Context, job *Job) error

	// Get 获取作业
	Get(ctx context.Context, jobID string) (*Job, error)

	// Delete 删除作业及其索引
	Delete(ctx context.Context, jobID string) error

	// PopPending 原子取出队列中最早到期的一个 pending 作业（scheduled_at <= now）并标记 running，
	// 无到期作业时返回 (nil, nil)。
	PopPending(ctx context.Context, queue string) (*Job, error)

	// Heartbeat 更新 running 作业的心跳与进度，返回作业是否仍处于 running 状态。
	// progress < 0 表示不更新进度，仅刷新心跳。
	Heartbeat(ctx context.Context, queue, jobID string, progress int) (bool, error)

	// Complete 标记 running 作业为 success 并写入结果。
	// 作业不在 running 状态（如已被取消/完成）时返回 ErrJobStateConflict。
	Complete(ctx context.Context, queue, jobID string, result []byte) error

	// Fail 标记 running 作业为 failed（最终失败），并记录错误信息。
	// 作业不在 running 状态时返回 ErrJobStateConflict。
	Fail(ctx context.Context, queue, jobID string, failure Failure) error

	// Retry 将失败的 running 作业重新入队为 pending，并将下次可执行时间设为 retryAt。
	// 作业不在 running 状态时返回 ErrJobStateConflict。
	Retry(ctx context.Context, queue, jobID string, retryAt time.Time, failure Failure) error

	// Cancel 取消作业（pending/running → cancelled）。
	// 幂等：作业已处于终态时返回 nil；作业不存在时返回 ErrJobNotFound。
	Cancel(ctx context.Context, queue, jobID string) error

	// Requeue 将失联的 running 作业重新入队（running → pending，立即可执行）。
	// 作业不在 running 状态时返回 ErrJobStateConflict。
	Requeue(ctx context.Context, queue, jobID string) error

	// ListStaleRunning 列出心跳早于 since 的 running 作业（用于失联恢复）
	ListStaleRunning(ctx context.Context, queue string, since time.Time, limit int64) ([]*Job, error)

	// GetStats 获取队列统计信息
	GetStats(ctx context.Context, queue string) (*Stats, error)

	// List 按过滤条件查询作业，结果按创建时间降序排列
	List(ctx context.Context, filter JobFilter) ([]*Job, error)

	// Cleanup 清理超过 retainFor 时长的终态作业（success/failed/cancelled），返回清理数量
	Cleanup(ctx context.Context, retainFor time.Duration) (int64, error)
}

// JobFilter 作业查询过滤条件，零值字段表示不限制
type JobFilter struct {
	Queue  string    // 队列过滤（空表示全部）
	Type   string    // 类型过滤（空表示全部）
	Status Status    // 状态过滤（空表示全部）
	Since  time.Time // 创建时间下限（零值表示不限）
	Until  time.Time // 创建时间上限（零值表示不限）
	Limit  int       // 返回数量上限（<=0 使用默认值 100）
	Offset int       // 偏移量
}

// Failure 作业执行失败信息
type Failure struct {
	Error  string         // 最近一次错误信息
	Errors []AttemptError // 完整错误历史
}

// Stats 队列统计信息
type Stats struct {
	Pending   int64 `json:"pending"`   // 等待执行数（含延迟/重试等待）
	Running   int64 `json:"running"`   // 执行中数
	Success   int64 `json:"success"`   // 成功累计数
	Failed    int64 `json:"failed"`    // 失败累计数
	Cancelled int64 `json:"cancelled"` // 取消累计数
}
