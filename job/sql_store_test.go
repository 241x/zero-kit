package job_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/241x/zero-kit/job"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// setupTestStore 创建基于 SQLite 的测试存储，返回存储实例和清理函数。
func setupTestStore(t *testing.T) (*job.SQLStore, func()) {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "test.db")), &gorm.Config{})
	require.NoError(t, err)

	store, err := job.NewSQLStore(db)
	require.NoError(t, err)

	cleanup := func() {
		sqlDB, _ := db.DB()
		sqlDB.Close()
	}
	return store, cleanup
}

func defaultTestConfig() job.Config {
	return job.DefaultConfig()
}

func TestSQLStore_SaveGet(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	j := job.NewJob("report", []byte("hello"))
	require.NoError(t, store.Save(context.Background(), j))

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Equal(t, j.ID, got.ID)
	assert.Equal(t, "report", got.Type)
	assert.Equal(t, []byte("hello"), got.Payload)
	assert.Equal(t, job.StatusPending, got.Status)
	assert.Equal(t, 1, got.MaxAttempts)
}

func TestSQLStore_GetNotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	_, err := store.Get(context.Background(), "missing")
	assert.ErrorIs(t, err, job.ErrJobNotFound)
}

func TestSQLStore_SaveNilJob(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	assert.ErrorIs(t, store.Save(context.Background(), nil), job.ErrNilJob)
}

func TestSQLStore_SaveEmptyType(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	assert.ErrorIs(t, store.Save(context.Background(), job.NewJob("", nil)), job.ErrEmptyJobType)
}

func TestSQLStore_PopPendingFIFO(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	first := job.NewJob("report", []byte("first"))
	second := job.NewJob("report", []byte("second"))
	require.NoError(t, store.Save(ctx, first))
	// 间隔足够让 UUIDv7 时间序字段产生差异
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, store.Save(ctx, second))

	got1, err := store.PopPending(ctx, job.DefaultQueue)
	require.NoError(t, err)
	require.NotNil(t, got1)
	assert.Equal(t, first.ID, got1.ID)
	assert.Equal(t, job.StatusRunning, got1.Status)
	assert.Equal(t, 1, got1.Attempts)

	got2, err := store.PopPending(ctx, job.DefaultQueue)
	require.NoError(t, err)
	require.NotNil(t, got2)
	assert.Equal(t, second.ID, got2.ID)

	got3, err := store.PopPending(ctx, job.DefaultQueue)
	require.NoError(t, err)
	assert.Nil(t, got3)
}

func TestSQLStore_PopPendingAtomic(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	got1, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	require.NotNil(t, got1)

	got2, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	assert.Nil(t, got2)
}

func TestSQLStore_Complete(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)

	require.NoError(t, store.Complete(ctx, j.Queue, j.ID, []byte("result")))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusSuccess, got.Status)
	assert.Equal(t, []byte("result"), got.Result)
	assert.NotZero(t, got.CompletedAt)
}

func TestSQLStore_Fail(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)

	failure := job.Failure{
		Error:  "boom",
		Errors: []job.AttemptError{{Attempt: 1, Error: "boom"}},
	}
	require.NoError(t, store.Fail(ctx, j.Queue, j.ID, failure))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusFailed, got.Status)
	assert.Equal(t, "boom", got.Error)
	require.Len(t, got.Errors, 1)
	assert.Equal(t, "boom", got.Errors[0].Error)
}

func TestSQLStore_CancelPending(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	require.NoError(t, store.Cancel(ctx, j.Queue, j.ID))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusCancelled, got.Status)
}

func TestSQLStore_Heartbeat(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)

	running, err := store.Heartbeat(ctx, j.Queue, j.ID, 42)
	require.NoError(t, err)
	assert.True(t, running)

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, 42, got.Progress)
	assert.NotZero(t, got.HeartbeatAt)
}

func TestSQLStore_HeartbeatNotRunning(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	running, err := store.Heartbeat(ctx, j.Queue, j.ID, 0)
	require.NoError(t, err)
	assert.False(t, running)
}

func TestSQLStore_Retry(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload")).WithMaxAttempts(3)
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)

	retryAt := time.Now().Add(10 * time.Second)
	require.NoError(t, store.Retry(ctx, j.Queue, j.ID, retryAt, job.Failure{Error: "boom"}))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusPending, got.Status)
	assert.Equal(t, "boom", got.Error)
	assert.Equal(t, retryAt.UnixMilli(), got.ScheduledAt)
}

func TestSQLStore_RetryBecomesPoppable(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload")).WithMaxAttempts(3)
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)

	// 重试时间已过，作业应立即可被再次弹出
	require.NoError(t, store.Retry(ctx, j.Queue, j.ID, time.Now().Add(-time.Second), job.Failure{Error: "boom"}))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusPending, got.Status)

	popped, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	require.NotNil(t, popped)
	assert.Equal(t, j.ID, popped.ID)
	assert.Equal(t, 2, popped.Attempts)
}

func TestSQLStore_Scheduled(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload")).WithDelay(50 * time.Millisecond)
	require.NoError(t, store.Save(ctx, j))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusPending, got.Status)
	assert.NotZero(t, got.ScheduledAt)

	// 未到期不会被弹出
	popped, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	assert.Nil(t, popped)

	// 到期后可被弹出
	time.Sleep(60 * time.Millisecond)
	popped, err = store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	require.NotNil(t, popped)
	assert.Equal(t, j.ID, popped.ID)
}

func TestSQLStore_Requeue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload")).WithMaxAttempts(3)
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)

	require.NoError(t, store.Requeue(ctx, j.Queue, j.ID))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusPending, got.Status)
}

func TestSQLStore_ListStaleRunning(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)

	jobs, err := store.ListStaleRunning(ctx, j.Queue, time.Now().Add(time.Second), 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, j.ID, jobs[0].ID)

	jobs, err = store.ListStaleRunning(ctx, j.Queue, time.Now().Add(-time.Minute), 10)
	require.NoError(t, err)
	assert.Empty(t, jobs)
}

func TestSQLStore_GetStats(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	require.NoError(t, store.Complete(ctx, j.Queue, j.ID, nil))

	stats, err := store.GetStats(ctx, j.Queue)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.Pending)
	assert.Equal(t, int64(0), stats.Running)
	assert.Equal(t, int64(1), stats.Success)
}

func TestSQLStore_Delete(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	require.NoError(t, store.Delete(ctx, j.ID))

	_, err := store.Get(ctx, j.ID)
	assert.ErrorIs(t, err, job.ErrJobNotFound)
}

func TestSQLStore_List(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	report1 := job.NewJob("report", []byte("r1"))
	report2 := job.NewJob("report", []byte("r2")).WithQueue("other")
	email := job.NewJob("email", []byte("e1"))
	require.NoError(t, store.Save(ctx, report1))
	require.NoError(t, store.Save(ctx, report2))
	require.NoError(t, store.Save(ctx, email))

	jobs, err := store.List(ctx, job.JobFilter{})
	require.NoError(t, err)
	assert.Len(t, jobs, 3)

	jobs, err = store.List(ctx, job.JobFilter{Queue: "other"})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, report2.ID, jobs[0].ID)

	jobs, err = store.List(ctx, job.JobFilter{Type: "report"})
	require.NoError(t, err)
	assert.Len(t, jobs, 2)
}

func TestSQLStore_ListByStatus(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	require.NoError(t, store.Complete(ctx, j.Queue, j.ID, nil))

	jobs, err := store.List(ctx, job.JobFilter{Status: job.StatusSuccess})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, j.ID, jobs[0].ID)

	jobs, err = store.List(ctx, job.JobFilter{Status: job.StatusPending})
	require.NoError(t, err)
	assert.Empty(t, jobs)
}

func TestSQLStore_Cleanup(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))
	_, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	require.NoError(t, store.Complete(ctx, j.Queue, j.ID, nil))

	n, err := store.Cleanup(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	_, err = store.Get(ctx, j.ID)
	assert.ErrorIs(t, err, job.ErrJobNotFound)
}

func TestSQLStore_CancelIdempotent(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	require.NoError(t, store.Cancel(ctx, j.Queue, j.ID))
	// 二次取消幂等，不应返回错误
	require.NoError(t, store.Cancel(ctx, j.Queue, j.ID))

	got, err := store.Get(ctx, j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusCancelled, got.Status)
}

func TestSQLStore_CancelNotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	assert.ErrorIs(t, store.Cancel(context.Background(), job.DefaultQueue, "missing"), job.ErrJobNotFound)
}

func TestSQLStore_CompleteStateConflict(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("payload"))
	require.NoError(t, store.Save(ctx, j))

	// 未抢占为 running 直接 Complete，应返回状态冲突而非 not found
	assert.ErrorIs(t, store.Complete(ctx, j.Queue, j.ID, nil), job.ErrJobStateConflict)
}

func TestSQLStore_ListDefaultLimit(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	for range 110 {
		require.NoError(t, store.Save(ctx, job.NewJob("report", []byte("x"))))
	}

	jobs, err := store.List(ctx, job.JobFilter{})
	require.NoError(t, err)
	assert.Len(t, jobs, 100)
}
