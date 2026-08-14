package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

// errPopConflict 抢占冲突，需重试
var errPopConflict = errors.New("pop conflict, retry")

// jobRecord 作业的持久化模型，通过 gorm 适配 MySQL。
// 仅支持 MySQL 5.7+（抢占依赖 UPDATE + RowsAffected 原子判定）。
// Payload/Result/Metadata/Errors 使用 JSON 列：可在数据库中通过 JSON_EXTRACT 等
// 结构化查询任务内容，写入时会校验 JSON 合法性（payload/result 需为合法 JSON）。
type jobRecord struct {
	ID          string `gorm:"column:id;primaryKey;size:64;comment:作业ID"`
	Queue       string `gorm:"column:queue;size:128;index:idx_queue_status,priority:1;comment:队列名"`
	Type        string `gorm:"column:type;size:128;index;comment:作业类型"`
	Payload     []byte `gorm:"column:payload;type:json;comment:作业输入数据（JSON）"`
	Status      string `gorm:"column:status;size:32;index:idx_queue_status,priority:2;comment:作业状态"`
	Progress    int    `gorm:"column:progress;comment:执行进度（0-100）"`
	Result      []byte `gorm:"column:result;type:json;comment:作业输出数据（JSON）"`
	Error       string `gorm:"column:error;type:text;comment:最近一次错误信息"`
	Attempts    int    `gorm:"column:attempts;comment:已执行次数"`
	MaxAttempts int    `gorm:"column:max_attempts;comment:最大执行次数"`
	ScheduledAt int64  `gorm:"column:scheduled_at;index:idx_queue_status,priority:3;comment:下次可执行时间（Unix 毫秒）"`
	CreatedAt   int64  `gorm:"column:created_at;index;comment:创建时间（Unix 毫秒）"`
	StartedAt   int64  `gorm:"column:started_at;comment:最近开始执行时间（Unix 毫秒）"`
	CompletedAt int64  `gorm:"column:completed_at;index;comment:完成时间（Unix 毫秒）"`
	HeartbeatAt int64  `gorm:"column:heartbeat_at;index;comment:最近心跳时间（Unix 毫秒）"`
	Metadata    string `gorm:"column:metadata;type:json;comment:元数据（JSON）"`
	Errors      string `gorm:"column:errors;type:json;comment:错误历史（JSON）"`
}

// TableName 指定表名
func (jobRecord) TableName() string {
	return "jobs"
}

// SQLStore 基于 MySQL 的作业存储实现，通过 gorm 适配。
// 仅支持 MySQL 5.7+，库与连接需使用 utf8mb4 字符集。
type SQLStore struct {
	db        *gorm.DB
	tableName string
}

// defaultTableName 默认作业表名
const defaultTableName = "jobs"

// SQLStoreOption 配置 SQLStore 的选项
type SQLStoreOption func(*SQLStore)

// WithTableName 设置作业表名（默认 "jobs"），用于多服务共库或表名前缀等场景
func WithTableName(name string) SQLStoreOption {
	return func(s *SQLStore) {
		s.tableName = name
	}
}

// NewSQLStore 创建 MySQL 作业存储实例并自动迁移表结构。
// db 由调用方通过项目的 mysql 包创建并管理生命周期，需保证数据库字符集为 utf8mb4。
// 可通过 WithTableName 等选项自定义表名。
func NewSQLStore(db *gorm.DB, opts ...SQLStoreOption) (*SQLStore, error) {
	if db == nil {
		return nil, errors.New("gorm db cannot be nil")
	}
	store := &SQLStore{
		db:        db,
		tableName: defaultTableName,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	if store.tableName == "" {
		store.tableName = defaultTableName
	}
	// 显式指定 utf8mb4 字符集建表，避免沿用库/连接默认字符集导致中文/emoji 乱码。
	// 连接字符集仍由调用方的 DSN 决定（见包注释），建表字符集此处兜底。
	if err := db.
		Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci").
		Table(store.tableName).AutoMigrate(&jobRecord{}); err != nil {
		return nil, fmt.Errorf("migrate job table failed: %w", err)
	}
	return store, nil
}

// scope 返回带上下文与表名的查询构造器
func (s *SQLStore) scope(ctx context.Context) *gorm.DB {
	return s.db.WithContext(ctx).Table(s.tableName)
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

	if err := s.scope(ctx).Create(rec).Error; err != nil {
		return fmt.Errorf("save job failed: %w", err)
	}
	return nil
}

// Get 获取作业
func (s *SQLStore) Get(ctx context.Context, jobID string) (*Job, error) {
	var rec jobRecord
	err := s.scope(ctx).Where("id = ?", jobID).First(&rec).Error
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
	res := s.scope(ctx).Where("id = ?", jobID).Delete(&jobRecord{})
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
			// 抢占冲突：让出 CPU 避免忙等，同时尊重 ctx 取消
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				runtime.Gosched()
				continue
			}
		}
		return job, err
	}
}

// popOnce 单次抢占，返回作业；空队列返回 (nil, nil)，抢占冲突返回 errPopConflict。
//
// MySQL 5.7 无 SKIP LOCKED，采用两段式抢占：
//  1. 无锁 SELECT 定位候选作业（走 (queue,status,scheduled_at) 复合索引）；
//  2. 原子 UPDATE 携带 status='pending' 条件，RowsAffected==1 才代表抢占成功，
//     并发下最多一个 worker 成功，其余等锁后 RowsAffected==0 转入冲突重试。
//
// UPDATE 单条自动提交、锁持有时间极短；1205/1213（锁等待/死锁）视为冲突重试。
func (s *SQLStore) popOnce(ctx context.Context, queue string) (*Job, error) {
	var rec jobRecord

	now := time.Now()

	// 无锁定位候选作业；Session(NewDB) 会清空 Statement，需重新指定表名
	sel := s.scope(ctx).Session(&gorm.Session{NewDB: true}).Table(s.tableName)
	err := sel.Where("queue = ? AND status = ? AND scheduled_at <= ?", queue, StatusPending, now.UnixMilli()).
		Order("scheduled_at ASC, created_at ASC, id ASC").
		Limit(1).
		First(&rec).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pop pending failed: %w", err)
	}

	// 原子抢占：仅当仍为 pending 且 attempts 未被并发修改时生效。
	// attempts 作为乐观锁版本号：UPDATE 等锁后重新评估 WHERE，
	// 若作业已被其他 worker 抢占并 Retry 回 pending，attempts 已变化，
	// 旧快照的 UPDATE 将不匹配，避免同一逻辑尝试被并发执行两次。
	res := s.scope(ctx).Model(&jobRecord{}).
		Where("id = ? AND status = ? AND attempts = ?", rec.ID, StatusPending, rec.Attempts).
		Updates(map[string]any{
			"status":       string(StatusRunning),
			"attempts":     rec.Attempts + 1,
			"started_at":   now.UnixMilli(),
			"heartbeat_at": now.UnixMilli(),
		})
	if res.Error != nil {
		if isLockError(res.Error) {
			return nil, errPopConflict
		}
		return nil, fmt.Errorf("pop pending failed: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// 作业已被其他 worker 抢占或状态异常
		return nil, errPopConflict
	}

	rec.Status = string(StatusRunning)
	rec.Attempts++
	rec.StartedAt = now.UnixMilli()
	rec.HeartbeatAt = now.UnixMilli()
	return recordToJob(&rec), nil
}

// isLockError 判断是否为 MySQL 锁等待超时（1205）或死锁（1213），这类错误可重试
func isLockError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
}

// Heartbeat 更新心跳与进度，返回是否仍处于 running 状态
func (s *SQLStore) Heartbeat(ctx context.Context, queue, jobID string, attempt, progress int) (bool, error) {
	updates := map[string]any{"heartbeat_at": time.Now().UnixMilli()}
	if progress >= 0 {
		updates["progress"] = progress
	}

	res := s.scope(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ? AND attempts = ?", queue, jobID, StatusRunning, attempt).
		Updates(updates)
	if res.Error != nil {
		return false, fmt.Errorf("heartbeat failed: %w", res.Error)
	}
	return res.RowsAffected > 0, nil
}

// Complete 标记作业成功
func (s *SQLStore) Complete(ctx context.Context, queue, jobID string, attempt int, result []byte) error {
	updates := map[string]any{
		"status":       string(StatusSuccess),
		"completed_at": time.Now().UnixMilli(),
		"error":        "", // 清空最近错误，成功作业不残留重试阶段的失败信息
	}
	if len(result) > 0 {
		updates["result"] = result
	}

	res := s.scope(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ? AND attempts = ?", queue, jobID, StatusRunning, attempt).
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
func (s *SQLStore) Fail(ctx context.Context, queue, jobID string, attempt int, failure Failure) error {
	errorsJSON, _ := json.Marshal(failure.Errors)
	res := s.scope(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ? AND attempts = ?", queue, jobID, StatusRunning, attempt).
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
func (s *SQLStore) Retry(ctx context.Context, queue, jobID string, attempt int, retryAt time.Time, failure Failure) error {
	errorsJSON, _ := json.Marshal(failure.Errors)
	res := s.scope(ctx).Model(&jobRecord{}).
		Where("queue = ? AND id = ? AND status = ? AND attempts = ?", queue, jobID, StatusRunning, attempt).
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
	res := s.scope(ctx).Model(&jobRecord{}).
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
	if err := s.scope(ctx).Model(&jobRecord{}).Where("queue = ? AND id = ?", queue, jobID).Count(&count).Error; err != nil {
		return fmt.Errorf("cancel job failed: %w", err)
	}
	if count == 0 {
		return ErrJobNotFound
	}
	return nil
}

// Requeue 将失联作业重新入队（running → pending，立即可执行）
func (s *SQLStore) Requeue(ctx context.Context, queue, jobID string) error {
	res := s.scope(ctx).Model(&jobRecord{}).
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
	err := s.scope(ctx).
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
	err := s.scope(ctx).Model(&jobRecord{}).
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
	query := s.scope(ctx).Model(&jobRecord{})

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

	res := s.scope(ctx).
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
