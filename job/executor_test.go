package job_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/241x/zero-kit/job"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestExecutor 创建基于 MySQL 的执行器及存储，未配置 DSN 时跳过。
func setupTestExecutor(t *testing.T, config job.Config, handler job.Handler) (*job.Executor, *job.SQLStore, func()) {
	t.Helper()

	db := openTestDB(t)
	table := uniqueTableName()

	store, err := job.NewSQLStore(db, job.WithTableName(table))
	require.NoError(t, err)
	executor := job.NewExecutor(store, handler, config)

	cleanup := func() {
		executor.Stop()
		dropTestTable(t, db, table)
	}

	return executor, store, cleanup
}

// fastRetryConfig 返回重试退避较短、扫描频繁的测试配置
func fastRetryConfig() job.Config {
	return job.DefaultConfig().
		WithHeartbeatInterval(30 * time.Millisecond).
		WithRetryDelay(50 * time.Millisecond).
		WithRetryStrategy(job.RetryStrategyFixed)
}

func TestExecutor_ExecuteSuccess(t *testing.T) {
	executor, store, cleanup := setupTestExecutor(t, defaultTestConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		j.Result = []byte("\"done\"")
		return nil
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusSuccess
	}, 3*time.Second, 20*time.Millisecond)

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Equal(t, []byte("\"done\""), got.Result)
	assert.Equal(t, 1, got.Attempts)
}

func TestExecutor_ExecuteFailure(t *testing.T) {
	executor, store, cleanup := setupTestExecutor(t, defaultTestConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return assert.AnError
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusFailed
	}, 3*time.Second, 20*time.Millisecond)

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	require.Len(t, got.Errors, 1)
}

func TestExecutor_RetryThenSucceed(t *testing.T) {
	var attempts atomic.Int32
	executor, store, cleanup := setupTestExecutor(t, fastRetryConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		if attempts.Add(1) <= 2 {
			return assert.AnError
		}
		return nil
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\"")).WithMaxAttempts(3)
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusSuccess
	}, 5*time.Second, 20*time.Millisecond)

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Equal(t, 3, got.Attempts)
	assert.Len(t, got.Errors, 2)
}

func TestExecutor_RetryExhausted(t *testing.T) {
	executor, store, cleanup := setupTestExecutor(t, fastRetryConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return assert.AnError
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\"")).WithMaxAttempts(2)
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusFailed
	}, 5*time.Second, 20*time.Millisecond)

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, got.Attempts)
	assert.Len(t, got.Errors, 2)
}

func TestExecutor_ProgressReporting(t *testing.T) {
	config := defaultTestConfig().WithHeartbeatInterval(30 * time.Millisecond)
	executor, store, cleanup := setupTestExecutor(t, config, job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		job.ReportProgress(ctx, 50)
		time.Sleep(100 * time.Millisecond)
		return nil
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusSuccess
	}, 3*time.Second, 20*time.Millisecond)

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Equal(t, 50, got.Progress)
}

func TestExecutor_ScheduledJob(t *testing.T) {
	executor, store, cleanup := setupTestExecutor(t, fastRetryConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return nil
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\"")).WithDelay(100 * time.Millisecond)
	require.NoError(t, store.Save(context.Background(), j))

	// 未到期时仍处于 pending，不会被取出执行
	time.Sleep(30 * time.Millisecond)
	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Equal(t, job.StatusPending, got.Status)

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusSuccess
	}, 3*time.Second, 20*time.Millisecond)
}

func TestExecutor_CancelViaStop(t *testing.T) {
	started := make(chan struct{})
	executor, store, cleanup := setupTestExecutor(t, defaultTestConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	<-started
	require.NoError(t, executor.Stop())
	assert.False(t, executor.IsRunning())
}

func TestExecutor_HandlerPanic(t *testing.T) {
	executor, store, cleanup := setupTestExecutor(t, defaultTestConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		panic("boom")
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusFailed
	}, 3*time.Second, 20*time.Millisecond)

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Contains(t, got.Error, "handler panic")
}

func TestExecutor_StartInvalidConcurrency(t *testing.T) {
	executor, _, cleanup := setupTestExecutor(t, defaultTestConfig().WithConcurrency(0), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return nil
	}))
	defer cleanup()

	err := executor.Start(context.Background())
	require.Error(t, err)
}

func TestExecutor_JobTimeout(t *testing.T) {
	config := defaultTestConfig().WithJobTimeout(100 * time.Millisecond)
	executor, store, cleanup := setupTestExecutor(t, config, job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusFailed
	}, 3*time.Second, 20*time.Millisecond)

	got, err := store.Get(context.Background(), j.ID)
	require.NoError(t, err)
	assert.Contains(t, got.Error, "context deadline exceeded")
}

func TestExecutor_StaleRecovery(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	j := job.NewJob("report", []byte("\"payload\"")).WithMaxAttempts(2)
	require.NoError(t, store.Save(ctx, j))

	// 手动抢占为 running，模拟执行中进程崩溃（无后续心跳）
	popped, err := store.PopPending(ctx, j.Queue)
	require.NoError(t, err)
	require.NotNil(t, popped)
	assert.Equal(t, job.StatusRunning, popped.Status)

	config := job.DefaultConfig().
		WithStaleTimeout(10 * time.Millisecond).
		WithRecoverInterval(20 * time.Millisecond).
		WithHeartbeatInterval(5 * time.Millisecond)

	var executed atomic.Int32
	executor := job.NewExecutor(store, job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		executed.Add(1)
		return nil
	}), config)
	require.NoError(t, executor.Start(context.Background()))
	defer executor.Stop()

	require.Eventually(t, func() bool {
		got, err := store.Get(ctx, j.ID)
		return err == nil && got.Status == job.StatusSuccess
	}, 3*time.Second, 20*time.Millisecond)

	assert.Equal(t, int32(1), executed.Load())
}

func TestExecutor_StartNilDependencies(t *testing.T) {
	executor := job.NewExecutor(nil, job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return nil
	}), defaultTestConfig())
	require.Error(t, executor.Start(context.Background()))

	executor = job.NewExecutor(&job.SQLStore{}, nil, defaultTestConfig())
	require.Error(t, executor.Start(context.Background()))
}

func TestExecutor_StopTimeout(t *testing.T) {
	config := defaultTestConfig().WithShutdownTimeout(50 * time.Millisecond)
	executor, store, cleanup := setupTestExecutor(t, config, job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		// 故意不响应取消，模拟阻塞处理器
		time.Sleep(time.Minute)
		return nil
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusRunning
	}, 3*time.Second, 20*time.Millisecond)

	err := executor.Stop()
	require.ErrorIs(t, err, job.ErrStopTimeout)
	assert.False(t, executor.IsRunning())
}

func TestExecutor_TypeDispatch(t *testing.T) {
	executor, store, cleanup := setupTestExecutor(t, defaultTestConfig(), job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		t.Fatalf("default handler should not be called for type %q", j.Type)
		return nil
	}))
	defer cleanup()

	var called atomic.Int32
	executor.Register("report", job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		called.Add(1)
		j.Result = []byte("\"report-done\"")
		return nil
	}))
	executor.Register("email", job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		called.Add(1)
		j.Result = []byte("\"email-done\"")
		return nil
	}))

	require.NoError(t, executor.Start(context.Background()))

	report := job.NewJob("report", []byte("\"r\""))
	email := job.NewJob("email", []byte("\"e\""))
	require.NoError(t, store.Save(context.Background(), report))
	require.NoError(t, store.Save(context.Background(), email))

	require.Eventually(t, func() bool {
		r, rErr := store.Get(context.Background(), report.ID)
		e, eErr := store.Get(context.Background(), email.ID)
		return rErr == nil && eErr == nil && r.Status == job.StatusSuccess && e.Status == job.StatusSuccess
	}, 3*time.Second, 20*time.Millisecond)

	r, _ := store.Get(context.Background(), report.ID)
	e, _ := store.Get(context.Background(), email.ID)
	assert.Equal(t, []byte("\"report-done\""), r.Result)
	assert.Equal(t, []byte("\"email-done\""), e.Result)
	assert.Equal(t, int32(2), called.Load())
}

func TestExecutor_NoHandlerFails(t *testing.T) {
	db := openTestDB(t)
	table := uniqueTableName()
	store, err := job.NewSQLStore(db, job.WithTableName(table))
	require.NoError(t, err)
	defer dropTestTable(t, db, table)

	// 无默认处理器，仅注册 report 类型
	executor := job.NewExecutor(store, nil, fastRetryConfig())
	executor.Register("report", job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return nil
	}))
	require.NoError(t, executor.Start(context.Background()))
	defer executor.Stop()

	// 默认 MaxAttempts=1，无 handler 的作业应立即失败
	j := job.NewJob("unknown", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusFailed
	}, 3*time.Second, 20*time.Millisecond)

	got, _ := store.Get(context.Background(), j.ID)
	assert.Contains(t, got.Error, "no handler")
}

func TestExecutor_NoHandlerRetriesThenFails(t *testing.T) {
	db := openTestDB(t)
	table := uniqueTableName()
	store, err := job.NewSQLStore(db, job.WithTableName(table))
	require.NoError(t, err)
	defer dropTestTable(t, db, table)

	executor := job.NewExecutor(store, nil, fastRetryConfig())
	executor.Register("report", job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		return nil
	}))
	require.NoError(t, executor.Start(context.Background()))
	defer executor.Stop()

	// MaxAttempts=2：无 handler 时先延迟重试一次，再最终失败
	j := job.NewJob("unknown", []byte("\"payload\"")).WithMaxAttempts(2)
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusFailed && got.Attempts == 2
	}, 5*time.Second, 20*time.Millisecond)

	got, _ := store.Get(context.Background(), j.ID)
	assert.Contains(t, got.Error, "no handler")
}

func TestExecutor_ProgressThrottleFlushesFinal(t *testing.T) {
	config := defaultTestConfig().WithProgressInterval(time.Second)
	executor, store, cleanup := setupTestExecutor(t, config, job.HandlerFunc(func(ctx context.Context, j *job.Job) error {
		job.ReportProgress(ctx, 10)
		job.ReportProgress(ctx, 40)
		job.ReportProgress(ctx, 90) // 最后一次在节流窗口内，依赖终态前 flush
		return nil
	}))
	defer cleanup()

	require.NoError(t, executor.Start(context.Background()))

	j := job.NewJob("report", []byte("\"payload\""))
	require.NoError(t, store.Save(context.Background(), j))

	require.Eventually(t, func() bool {
		got, err := store.Get(context.Background(), j.ID)
		return err == nil && got.Status == job.StatusSuccess
	}, 3*time.Second, 20*time.Millisecond)

	got, _ := store.Get(context.Background(), j.ID)
	assert.Equal(t, 90, got.Progress)
}
