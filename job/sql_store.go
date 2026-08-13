package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// errPopConflict 抢占冲突，需重试
var errPopConflict = errors.New("pop conflict, retry")

// jobRecord 作业的持久化模型，通过 gorm 适配 SQLite / MySQL 等关系型数据库。
type jobRecord struct {
	ID          string `gorm:"column:id;primaryKey;size:64"`
	Queue       string `gorm:"column:queue;size:128;index:idx_queue_status,priority:1"`
	Type        string `gorm:"column:type;size:128;index"`
	Payload     []byte `gorm:"column:payload"`
	Status      string `gorm:"column:status;size:32;index:idx_queue_status,priority:2"`
	Progress    int    `gorm:"column:progress"`
	Result      []byte `gorm:"column:result"`
	Error       string `gorm:"column:error;type:text"`
	Attempts    int    `gorm:"column:attempts"`
	MaxAttempts int    `gorm:"column:max_attempts"`
	ScheduledAt int64  `gorm:"column:scheduled_at;index"`
	CreatedAt   int64  `gorm:"column:created_at;index"`
	StartedAt   int64  `gorm:"column:started_at"`
	CompletedAt int64  `gorm:"column:completed_at;index"`
	HeartbeatAt int64  `gorm:"column:heartbeat_at;index"`
	Metadata    string `gorm:"column:metadata;type:text"`
	Errors      string `gorm:"column:errors;type:text"`
}

// TableName 指定表名
func (jobRecord) TableName() string {
	return "jobs"
}

// SQLStore 基于关系型数据库的作业存储实现，通过 gorm 适配 SQLite / MySQL。
type SQLStore struct {
	db      *gorm.DB
	dialect string
}

// NewSQLStore 创建 SQL 作业存储实例并自动迁移表结构。
// db 由调用方通过项目的 sqlite / mysql 包创建并管理生命周期。
func NewSQLStore(db *gorm.DB) (*SQLStore, error) {
	if db == nil {
		return nil, errors.New("gorm db cannot be nil")
	}
	if err := db.AutoMigrate(&jobRecord{}); err != nil {
		return nil, fmt.Errorf("migrate job table failed: %w", err)
	}
	return &SQLStore{
		db:      db,
		dialect: db.Dialector.Name(),
	}, nil
}

// Save 创建作业并按调度时间设置初始状态。
// 不修改传入的 job：默认队列、重试次数与初始状态仅作用于落库数据，调用方应通过 Get 读取最终值。
func (s *SQLStore) Save(ctx context.Context, job *Job) error {
	if job == nil {
		return ErrNilJob
	}
	if job.Type == "" {
		return ErrEmptyJobType
	}

	queue := job.Queue
	if queue == "" {
		queue = DefaultQueue
	}
	maxAttempts := job.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	rec := jobToRecord(job)
	rec.Queue = queue
	rec.MaxAttempts = maxAttempts
	rec.Status = string(StatusPending)

	// 立即执行的作业归一化为当前时间，保证按到期时间排序时不会被后续延迟作业插队
	now := time.Now().UnixMilli()
	if rec.ScheduledAt <= now {
		rec.ScheduledAt = now
	}

	if err := s.db.WithContext(ctx).Create(rec).Error; err != nil {
		return fmt.Errorf("save job failed: %w", err)
	}
	return nil
}

// Get 获取作业
func (s *SQLStore) Get(ctx context.Context, jobID string) (*Job, error) {
	var rec jobRecord
	err := s.db.WithContext(ctx).Where("id = ?", jobID).First(&rec).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, fmt.Errorf("get job failed: %w", err)
	}
	return recordToJob(&rec), nil
}

// Delete 删除作业
func (s *SQLStore) Delete(ctx context.Context, jobID string) error {
	res := s.db.WithContext(ctx).Where("id = ?", jobID).Delete(&jobRecord{})
	if res.Error != nil {
		return fmt.Errorf("delete job failed: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrJobNotFound
	}
	return nil
}

// PopPending 原子取出最早的一个 pending 作业并标记 running
func (s *SQLStore) PopPending(ctx context.Context, queue string) (*Job, error) {
	for {
		job, err := s.popOnce(ctx, queue)
		if errors.Is(err, errPopConflict) {
			// 抢占冲突：尊重 ctx 取消，避免忙等
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				continue
			}
		}
		return job, err
	}
}

// popOnce 单次抢占，返回作业；空队列返回 (nil, nil)，抢占冲突返回 errPopConflict
func (s *SQLStore) popOnce(ctx context.Context, queue string) (*Job, error) {
	var rec jobRecord

	now := time.Now()
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Where("queue = ? AND status = ? AND scheduled_at <= ?", queue, StatusPending, now.UnixMilli()).
			Order("scheduled_at ASC, created_at ASC, id ASC").Limit(1)

		// MySQL 使用 SKIP LOCKED 跳过已锁定行，降低并发抢占冲突
		if s.dialect == "mysql" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
		}

		if err := query.First(&rec).Error; err != nil {
			return err
		}

		res := tx.Model(&jobRecord{}).
			Where("id = ? AND status = ?", rec.ID, StatusPending).
			Updates(map[string]any{
				"status":       string(StatusRunning),
				"attempts":     rec.Attempts + 1,
				"started_at":   now.UnixMilli(),
				"heartbeat_at": now.UnixMilli(),
			})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errPopConflict
		}

		rec.Status = string(StatusRunning)
		rec.Attempts++
		rec.StartedAt = now.UnixMilli()
		rec.HeartbeatAt = now.UnixMilli()
		return nil
	})

	if errors.Is(err, errPopConflict) {
		return nil, errPopConflict
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pop pending failed: %w", err)
	}
	return recordToJob(&rec), nil
}

// Heartbeat 更新心跳与进度，返回是否仍处于 running 状态
func (s *SQLStore) Heartbeat(ctx context.Context, queue, jobID string, progress int) (bool, error) {
	updates := map[string]any{"heartbeat_at": time.Now().UnixMilli()}
	if progress >= 0 {
		updates["progress"] = progress
	}

	res := s.db.WithContext(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ?", queue, jobID, StatusRunning).
		Updates(updates)
	if res.Error != nil {
		return false, fmt.Errorf("heartbeat failed: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// Complete 标记作业成功
func (s *SQLStore) Complete(ctx context.Context, queue, jobID string, result []byte) error {
	updates := map[string]any{
		"status":       string(StatusSuccess),
		"completed_at": time.Now().UnixMilli(),
	}
	if len(result) > 0 {
		updates["result"] = result
	}

	res := s.db.WithContext(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ?", queue, jobID, StatusRunning).
		Updates(updates)
	if res.Error != nil {
		return fmt.Errorf("complete job failed: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrJobStateConflict
	}
	return nil
}

// Fail 标记作业最终失败
func (s *SQLStore) Fail(ctx context.Context, queue, jobID string, failure Failure) error {
	errorsJSON, _ := json.Marshal(failure.Errors)
	res := s.db.WithContext(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ?", queue, jobID, StatusRunning).
		Updates(map[string]any{
			"status":       string(StatusFailed),
			"completed_at": time.Now().UnixMilli(),
			"error":        failure.Error,
			"errors":       string(errorsJSON),
		})
	if res.Error != nil {
		return fmt.Errorf("fail job failed: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrJobStateConflict
	}
	return nil
}

// Retry 将失败的 running 作业重新入队为 pending，并将下次可执行时间设为 retryAt
func (s *SQLStore) Retry(ctx context.Context, queue, jobID string, retryAt time.Time, failure Failure) error {
	errorsJSON, _ := json.Marshal(failure.Errors)
	res := s.db.WithContext(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ?", queue, jobID, StatusRunning).
		Updates(map[string]any{
			"status":       string(StatusPending),
			"scheduled_at": retryAt.UnixMilli(),
			"error":        failure.Error,
			"errors":       string(errorsJSON),
		})
	if res.Error != nil {
		return fmt.Errorf("retry job failed: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrJobStateConflict
	}
	return nil
}

// Cancel 取消作业（pending/running → cancelled）。
// 幂等：作业已处于终态（success/failed/cancelled）时返回 nil；作业不存在时返回 ErrJobNotFound。
func (s *SQLStore) Cancel(ctx context.Context, queue, jobID string) error {
	res := s.db.WithContext(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status IN ?", queue, jobID,
			[]string{string(StatusPending), string(StatusRunning)}).
		Updates(map[string]any{
			"status":       string(StatusCancelled),
			"completed_at": time.Now().UnixMilli(),
		})
	if res.Error != nil {
		return fmt.Errorf("cancel job failed: %w", res.Error)
	}
	if res.RowsAffected > 0 {
		return nil
	}

	// 未影响任何行：区分「作业不存在」与「已处于终态」。
	var count int64
	if err := s.db.WithContext(ctx).Model(&jobRecord{}).Where("id = ?", jobID).Count(&count).Error; err != nil {
		return fmt.Errorf("cancel job failed: %w", err)
	}
	if count == 0 {
		return ErrJobNotFound
	}
	return nil
}

// Requeue 将失联作业重新入队（running → pending，立即可执行）
func (s *SQLStore) Requeue(ctx context.Context, queue, jobID string) error {
	res := s.db.WithContext(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ?", queue, jobID, StatusRunning).
		Updates(map[string]any{
			"status":       string(StatusPending),
			"scheduled_at": time.Now().UnixMilli(),
			"started_at":   int64(0),
			"error":        "",
		})
	if res.Error != nil {
		return fmt.Errorf("requeue job failed: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrJobStateConflict
	}
	return nil
}

// ListStaleRunning 列出心跳早于 since 的 running 作业
func (s *SQLStore) ListStaleRunning(ctx context.Context, queue string, since time.Time, limit int64) ([]*Job, error) {
	var recs []jobRecord
	err := s.db.WithContext(ctx).
		Where("queue = ? AND status = ? AND heartbeat_at <= ?", queue, StatusRunning, since.UnixMilli()).
		Order("heartbeat_at ASC").
		Limit(int(limit)).
		Find(&recs).Error
	if err != nil {
		return nil, fmt.Errorf("list stale running failed: %w", err)
	}

	jobs := make([]*Job, 0, len(recs))
	for i := range recs {
		jobs = append(jobs, recordToJob(&recs[i]))
	}
	return jobs, nil
}

// GetStats 获取队列统计信息（按当前存在的各状态作业数）
func (s *SQLStore) GetStats(ctx context.Context, queue string) (*Stats, error) {
	stats := &Stats{}

	type statusCount struct {
		Status string `gorm:"column:status"`
		Count  int64  `gorm:"column:count"`
	}
	var counts []statusCount
	err := s.db.WithContext(ctx).Model(&jobRecord{}).
		Select("status, count(*) as count").
		Where("queue = ?", queue).
		Group("status").
		Scan(&counts).Error
	if err != nil {
		return nil, fmt.Errorf("get stats failed: %w", err)
	}

	for _, c := range counts {
		switch Status(c.Status) {
		case StatusPending:
			stats.Pending = c.Count
		case StatusRunning:
			stats.Running = c.Count
		case StatusSuccess:
			stats.Success = c.Count
		case StatusFailed:
			stats.Failed = c.Count
		case StatusCancelled:
			stats.Cancelled = c.Count
		}
	}
	return stats, nil
}

// List 按过滤条件查询作业，结果按创建时间降序排列
func (s *SQLStore) List(ctx context.Context, filter JobFilter) ([]*Job, error) {
	query := s.db.WithContext(ctx).Model(&jobRecord{})

	if filter.Queue != "" {
		query = query.Where("queue = ?", filter.Queue)
	}
	if filter.Type != "" {
		query = query.Where("type = ?", filter.Type)
	}
	if filter.Status != "" {
		query = query.Where("status = ?", filter.Status)
	}
	if !filter.Since.IsZero() {
		query = query.Where("created_at >= ?", filter.Since.UnixMilli())
	}
	if !filter.Until.IsZero() {
		query = query.Where("created_at <= ?", filter.Until.UnixMilli())
	}

	query = query.Order("created_at DESC, id DESC")
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	query = query.Limit(limit)

	var recs []jobRecord
	if err := query.Find(&recs).Error; err != nil {
		return nil, fmt.Errorf("list jobs failed: %w", err)
	}

	jobs := make([]*Job, 0, len(recs))
	for i := range recs {
		jobs = append(jobs, recordToJob(&recs[i]))
	}
	return jobs, nil
}

// Cleanup 清理超过 retainFor 时长的终态作业
func (s *SQLStore) Cleanup(ctx context.Context, retainFor time.Duration) (int64, error) {
	before := time.Now().Add(-retainFor).UnixMilli()

	res := s.db.WithContext(ctx).
		Where("status IN ? AND completed_at <= ?",
			[]string{string(StatusSuccess), string(StatusFailed), string(StatusCancelled)}, before).
		Delete(&jobRecord{})
	if res.Error != nil {
		return 0, fmt.Errorf("cleanup jobs failed: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// jobToRecord 将领域作业转为持久化模型
func jobToRecord(job *Job) *jobRecord {
	metadata, _ := json.Marshal(job.Metadata)
	errorsJSON, _ := json.Marshal(job.Errors)
	return &jobRecord{
		ID:          job.ID,
		Queue:       job.Queue,
		Type:        job.Type,
		Payload:     job.Payload,
		Status:      string(job.Status),
		Progress:    job.Progress,
		Result:      job.Result,
		Error:       job.Error,
		Attempts:    job.Attempts,
		MaxAttempts: job.MaxAttempts,
		ScheduledAt: job.ScheduledAt,
		CreatedAt:   job.CreatedAt,
		StartedAt:   job.StartedAt,
		CompletedAt: job.CompletedAt,
		HeartbeatAt: job.HeartbeatAt,
		Metadata:    string(metadata),
		Errors:      string(errorsJSON),
	}
}

// recordToJob 将持久化模型转为领域作业
func recordToJob(rec *jobRecord) *Job {
	job := &Job{
		ID:          rec.ID,
		Queue:       rec.Queue,
		Type:        rec.Type,
		Payload:     rec.Payload,
		Status:      Status(rec.Status),
		Progress:    rec.Progress,
		Result:      rec.Result,
		Error:       rec.Error,
		Attempts:    rec.Attempts,
		MaxAttempts: rec.MaxAttempts,
		ScheduledAt: rec.ScheduledAt,
		CreatedAt:   rec.CreatedAt,
		StartedAt:   rec.StartedAt,
		CompletedAt: rec.CompletedAt,
		HeartbeatAt: rec.HeartbeatAt,
		Metadata:    make(map[string]string),
	}
	if rec.Metadata != "" {
		_ = json.Unmarshal([]byte(rec.Metadata), &job.Metadata)
	}
	if rec.Errors != "" {
		_ = json.Unmarshal([]byte(rec.Errors), &job.Errors)
	}
	return job
}
