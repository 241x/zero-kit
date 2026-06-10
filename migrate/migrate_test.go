package migrate_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/241x/zero-kit/logger"
	"github.com/241x/zero-kit/migrate"
)

// mockScriptReader Mock 脚本读取器
type mockScriptReader struct {
	content string
	err     error
}

func (m *mockScriptReader) Read() (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.content, nil
}

// mockProgressStore Mock 进度存储
type mockProgressStore struct {
	mu       sync.RWMutex
	lastLine int
	saveErr  error
	loadErr  error
	records  []int
}

func (m *mockProgressStore) Load() (int, error) {
	if m.loadErr != nil {
		return 0, m.loadErr
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastLine, nil
}

func (m *mockProgressStore) Save(lineNum int) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastLine = lineNum
	m.records = append(m.records, lineNum)
	return nil
}

func (m *mockProgressStore) getRecords() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]int, len(m.records))
	copy(result, m.records)
	return result
}

// setupTestDB 创建测试数据库
func setupTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)
	return db
}

// TestNewMigrator 测试构造函数
func TestNewMigrator(t *testing.T) {
	db := setupTestDB(t)

	t.Run("默认配置", func(t *testing.T) {
		m := migrate.NewMigrator(db, "test.sql")
		assert.NotNil(t, m)
	})

	t.Run("自定义选项", func(t *testing.T) {
		m := migrate.NewMigrator(db, "test.sql",
			migrate.WithLogger(logger.Nop()),
		)
		assert.NotNil(t, m)
	})
}

// TestMigrate_EmptyScript 测试空脚本
func TestMigrate_EmptyScript(t *testing.T) {
	db := setupTestDB(t)
	var buf bytes.Buffer
	log := logger.New(logger.WithLevel(logger.InfoLevel), logger.WithWriter(&buf))
	mockReader := &mockScriptReader{content: ""}
	mockProgress := &mockProgressStore{}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(log),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "No migrations to apply")
}

// TestMigrate_SingleStatement 测试单条语句
func TestMigrate_SingleStatement(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{
		content: `CREATE TABLE test_table (id INTEGER PRIMARY KEY);`,
	}
	mockProgress := &mockProgressStore{}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []int{1}, mockProgress.getRecords())

	// 验证表已创建
	var result int
	err = db.Raw("SELECT COUNT(*) FROM test_table").Scan(&result).Error
	assert.NoError(t, err)
}

// TestMigrate_MultipleStatements 测试多条语句
func TestMigrate_MultipleStatements(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{
		content: `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);

-- [CHECK POINT] --

INSERT INTO users (id, name) VALUES (1, 'test');

-- [CHECK POINT] --

UPDATE users SET name = 'updated' WHERE id = 1;`,
	}
	mockProgress := &mockProgressStore{}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []int{3, 7, 9}, mockProgress.getRecords())

	// 验证数据
	var name string
	err = db.Raw("SELECT name FROM users WHERE id = 1").Scan(&name).Error
	assert.NoError(t, err)
	assert.Equal(t, "updated", name)
}

// TestMigrate_WithCheckpoint 测试断点续执行
func TestMigrate_WithCheckpoint(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()

	// 先创建表（模拟之前已执行过建表语句）
	err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY);").Error
	assert.NoError(t, err)

	mockReader := &mockScriptReader{
		content: `CREATE TABLE users (id INTEGER PRIMARY KEY);

-- [CHECK POINT] --

INSERT INTO users (id) VALUES (1);

-- [CHECK POINT] --

INSERT INTO users (id) VALUES (2);`,
	}
	mockProgress := &mockProgressStore{lastLine: 4} // 从第4行开始，跳过建表语句

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err = m.Migrate(context.Background())
	assert.NoError(t, err)
	// 应该只执行后两条 INSERT
	assert.Equal(t, []int{7, 9}, mockProgress.getRecords())

	// 验证有两条数据
	var count int
	err = db.Raw("SELECT COUNT(*) FROM users").Scan(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

// TestMigrate_ScriptReaderError 测试脚本读取错误
func TestMigrate_ScriptReaderError(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{err: errors.New("read error")}
	mockProgress := &mockProgressStore{}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.Error(t, err)
	migrateErr, ok := errors.AsType[*migrate.MigrationError](err)
	assert.True(t, ok)
	assert.Contains(t, migrateErr.Op, "parseSQLStatements")
}

// TestMigrate_ProgressLoadError 测试进度加载错误
func TestMigrate_ProgressLoadError(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{content: "SELECT 1;"}
	mockProgress := &mockProgressStore{loadErr: errors.New("load error")}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.Error(t, err)
}

// TestMigrate_ProgressSaveError 测试进度保存错误
func TestMigrate_ProgressSaveError(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{content: "SELECT 1;"}
	mockProgress := &mockProgressStore{saveErr: errors.New("save error")}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.Error(t, err)
}

// TestMigrate_ContextCanceled 测试上下文取消
func TestMigrate_ContextCanceled(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{content: "SELECT 1;"}
	mockProgress := &mockProgressStore{}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	err := m.Migrate(ctx)
	assert.Equal(t, context.Canceled, err)
}

// TestMigrate_SQLError 测试 SQL 执行错误
func TestMigrate_SQLError(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{content: "INVALID SQL STATEMENT;"}
	mockProgress := &mockProgressStore{}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.Error(t, err)
	migrateErr, ok := errors.AsType[*migrate.MigrationError](err)
	assert.True(t, ok)
	assert.Contains(t, migrateErr.Op, "db.Exec")
}

// TestMigrate_CommentAndEmptyLines 测试注释和空行处理
func TestMigrate_CommentAndEmptyLines(t *testing.T) {
	db := setupTestDB(t)
	mockLog := logger.Nop()
	mockReader := &mockScriptReader{
		content: `-- This is a comment
CREATE TABLE test_table (id INTEGER PRIMARY KEY);

-- Another comment

-- [CHECK POINT] --

INSERT INTO test_table (id) VALUES (1);
`,
	}
	mockProgress := &mockProgressStore{}

	m := migrate.NewMigrator(db, "test.sql",
		migrate.WithLogger(mockLog),
		migrate.WithScriptReader(mockReader),
		migrate.WithProgressStore(mockProgress),
	)

	err := m.Migrate(context.Background())
	assert.NoError(t, err)
	// marker在第6行，INSERT在第9行（因为最后有换行）
	assert.Equal(t, []int{6, 9}, mockProgress.getRecords())
}

// TestMigrationError 测试错误类型
func TestMigrationError(t *testing.T) {
	innerErr := errors.New("inner error")
	migrateErr := &migrate.MigrationError{
		Op:  "testOp",
		Err: innerErr,
	}

	assert.Contains(t, migrateErr.Error(), "testOp")
	assert.Contains(t, migrateErr.Error(), "inner error")
	assert.Equal(t, innerErr, migrateErr.Unwrap())
	assert.True(t, errors.Is(migrateErr, innerErr))
}

// setupDBProgressStore 创建 DBProgressStore 测试环境
func setupDBProgressStore(t *testing.T) (*gorm.DB, *migrate.DBProgressStore) {
	db := setupTestDB(t)
	err := db.AutoMigrate(&migrate.MigrationProgress{})
	assert.NoError(t, err)
	store := migrate.NewDBProgressStore(db, "test_script.sql")
	return db, store
}

// TestDBProgressStore_LoadEmpty 测试首次 Load 返回 0
func TestDBProgressStore_LoadEmpty(t *testing.T) {
	_, store := setupDBProgressStore(t)
	lineNum, err := store.Load()
	assert.NoError(t, err)
	assert.Equal(t, 0, lineNum)
}

// TestDBProgressStore_SaveAndLoad 测试 Save 后 Load 能正确返回
func TestDBProgressStore_SaveAndLoad(t *testing.T) {
	_, store := setupDBProgressStore(t)

	err := store.Save(42)
	assert.NoError(t, err)

	lineNum, err := store.Load()
	assert.NoError(t, err)
	assert.Equal(t, 42, lineNum)
}

// TestDBProgressStore_MultipleSaves 测试多次 Save 后 Load 返回最新值
func TestDBProgressStore_MultipleSaves(t *testing.T) {
	_, store := setupDBProgressStore(t)

	for _, line := range []int{10, 20, 35, 50} {
		err := store.Save(line)
		assert.NoError(t, err)
	}

	lineNum, err := store.Load()
	assert.NoError(t, err)
	assert.Equal(t, 50, lineNum)
}

// TestDBProgressStore_DifferentScripts 测试不同脚本路径互不干扰
func TestDBProgressStore_DifferentScripts(t *testing.T) {
	db := setupTestDB(t)
	err := db.AutoMigrate(&migrate.MigrationProgress{})
	assert.NoError(t, err)

	storeA := migrate.NewDBProgressStore(db, "script_a.sql")
	storeB := migrate.NewDBProgressStore(db, "script_b.sql")

	assert.NoError(t, storeA.Save(100))
	assert.NoError(t, storeB.Save(200))

	lineA, err := storeA.Load()
	assert.NoError(t, err)
	assert.Equal(t, 100, lineA)

	lineB, err := storeB.Load()
	assert.NoError(t, err)
	assert.Equal(t, 200, lineB)
}

// TestDBProgressStore_SaveHistory 测试每次 Save 追加记录，保留完整迁移历史
func TestDBProgressStore_SaveHistory(t *testing.T) {
	db, store := setupDBProgressStore(t)

	lines := []int{10, 20, 35, 50}
	for _, line := range lines {
		err := store.Save(line)
		assert.NoError(t, err)
	}

	// 验证数据库中记录条数等于 Save 次数
	var count int64
	err := db.Model(&migrate.MigrationProgress{}).Where("script_path = ?", "test_script.sql").Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, int64(len(lines)), count)

	// 验证 Load 返回最新一条
	lineNum, err := store.Load()
	assert.NoError(t, err)
	assert.Equal(t, 50, lineNum)
}
