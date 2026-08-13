// Package job 提供长时间运行作业（Job）的状态机与执行能力。
//
// 与 queue 包（无状态消息投递）不同，job 包关注作业的完整生命周期：
// 提交（pending）→ 执行（running）→ 失败重试（pending + 调度时间）→ 终态（success/failed/cancelled），
// 并提供进度上报、心跳保活、失联恢复与重试退避等长任务必需的能力。
//
// 重要语义：作业采用「至少一次执行」投递，即作业可能被重复执行，
// 处理器（Handler）必须实现幂等或显式容忍重复。
package job

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// DefaultQueue 默认队列名
const DefaultQueue = "default"

// 作业相关公共错误
var (
	// ErrJobNotFound 作业不存在
	ErrJobNotFound = errors.New("job not found")
	// ErrNilJob 作业为空
	ErrNilJob = errors.New("job cannot be nil")
	// ErrEmptyJobType 作业类型为空
	ErrEmptyJobType = errors.New("job type cannot be empty")
	// ErrJobStateConflict 作业状态与预期不符，状态转移未生效
	ErrJobStateConflict = errors.New("job state conflict")
)

// Status 作业状态
type Status string

const (
	// StatusPending 等待执行（含延迟/重试等待，由 ScheduledAt 控制实际可执行时间）
	StatusPending Status = "pending"
	// StatusRunning 执行中
	StatusRunning Status = "running"
	// StatusSuccess 执行成功
	StatusSuccess Status = "success"
	// StatusFailed 执行失败
	StatusFailed Status = "failed"
	// StatusCancelled 已取消
	StatusCancelled Status = "cancelled"
)

// AttemptError 记录单次执行尝试的错误信息
type AttemptError struct {
	Attempt int    `json:"attempt"` // 尝试次数（1-based）
	At      int64  `json:"at"`      // 失败时间（Unix 毫秒）
	Error   string `json:"error"`   // 错误信息
}

// Job 表示一个长时间运行的作业
type Job struct {
	ID          string            `json:"id"`           // 作业ID
	Queue       string            `json:"queue"`        // 队列名
	Type        string            `json:"type"`         // 作业类型
	Payload     []byte            `json:"payload"`      // 作业输入数据
	Status      Status            `json:"status"`       // 作业状态
	Progress    int               `json:"progress"`     // 执行进度（0-100）
	Result      []byte            `json:"result"`       // 作业输出数据
	Error       string            `json:"error"`        // 最近一次错误信息
	Attempts    int               `json:"attempts"`     // 已执行次数（1-based，首次执行为 1）
	MaxAttempts int               `json:"max_attempts"` // 最大执行次数（默认 1，即不重试）
	ScheduledAt int64             `json:"scheduled_at"` // 下次可执行时间（Unix 毫秒，0 表示立即）
	CreatedAt   int64             `json:"created_at"`   // 创建时间（Unix 毫秒）
	StartedAt   int64             `json:"started_at"`   // 最近开始执行时间（Unix 毫秒）
	CompletedAt int64             `json:"completed_at"` // 完成时间（Unix 毫秒）
	HeartbeatAt int64             `json:"heartbeat_at"` // 最近心跳时间（Unix 毫秒）
	Metadata    map[string]string `json:"metadata"`     // 元数据
	Errors      []AttemptError    `json:"errors"`       // 错误历史
}

// NewJob 创建新作业
func NewJob(jobType string, payload []byte) *Job {
	return &Job{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Queue:       DefaultQueue,
		Type:        jobType,
		Payload:     payload,
		Status:      StatusPending,
		Progress:    0,
		Attempts:    0,
		MaxAttempts: 1,
		CreatedAt:   time.Now().UnixMilli(),
		Metadata:    make(map[string]string),
	}
}

// WithPayload 设置作业输入数据
func (j *Job) WithPayload(payload []byte) *Job {
	j.Payload = payload
	return j
}

// WithQueue 设置队列名
func (j *Job) WithQueue(queue string) *Job {
	j.Queue = queue
	return j
}

// WithMetadata 设置元数据
func (j *Job) WithMetadata(key, value string) *Job {
	if j.Metadata == nil {
		j.Metadata = make(map[string]string)
	}
	j.Metadata[key] = value
	return j
}

// WithMaxAttempts 设置最大执行次数（含首次，默认 1 表示不重试）
func (j *Job) WithMaxAttempts(maxAttempts int) *Job {
	j.MaxAttempts = maxAttempts
	return j
}

// WithScheduleAt 设置定时执行时间
func (j *Job) WithScheduleAt(t time.Time) *Job {
	j.ScheduledAt = t.UnixMilli()
	return j
}

// WithDelay 设置延迟执行（从当前时间起算）
func (j *Job) WithDelay(delay time.Duration) *Job {
	j.ScheduledAt = time.Now().Add(delay).UnixMilli()
	return j
}

// IsTerminal 报告作业是否处于终态（成功/失败/取消）
func (j *Job) IsTerminal() bool {
	return j.Status == StatusSuccess || j.Status == StatusFailed || j.Status == StatusCancelled
}
