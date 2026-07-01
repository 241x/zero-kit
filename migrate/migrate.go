package migrate

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/241x/zero-kit/logger"

	"gorm.io/gorm"
)

// MigrationError 自定义错误类型
type MigrationError struct {
	Op  string
	Err error
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("migration: %s: %v", e.Op, e.Err)
}

func (e *MigrationError) Unwrap() error {
	return e.Err
}

func wrapError(op string, err error) error {
	return &MigrationError{Op: op, Err: err}
}

// SQLStatement 表示一个待执行的 SQL 片段
type SQLStatement struct {
	LineNum int
	SQL     string
}

const (
	MigrationMarker       = "-- [CHECK POINT] --"
	TimeFormat            = "2006-01-02 15:04:05"
	DefaultProgressSuffix = ".progress"
)

// ScriptReader SQL 脚本读取接口
type ScriptReader interface {
	// Read 读取脚本内容
	Read() (string, error)
}

// ProgressStore 进度存储接口
type ProgressStore interface {
	// Load 加载上次迁移断点
	Load(ctx context.Context) (int, error)
	// Save 保存当前迁移进度
	Save(ctx context.Context, lineNum int) error
}

// FileScriptReader 文件脚本读取器
type FileScriptReader struct {
	path string
}

// NewFileScriptReader 创建文件脚本读取器
func NewFileScriptReader(path string) *FileScriptReader {
	return &FileScriptReader{path: path}
}

// Read 读取脚本内容
func (r *FileScriptReader) Read() (string, error) {
	content, err := os.ReadFile(r.path)
	if err != nil {
		return "", wrapError("os.ReadFile", err)
	}
	return string(content), nil
}

// FileProgressStore 文件进度存储
type FileProgressStore struct {
	path       string
	timeFormat string
}

// MigrationProgress 迁移进度记录（数据库存储模型）
type MigrationProgress struct {
	ID         uint   `gorm:"primaryKey"`
	ScriptPath string `gorm:"index;size:255;not null"`
	LineNum    int    `gorm:"not null"`
	UpdatedAt  time.Time
}

// DBProgressStore 数据库进度存储
type DBProgressStore struct {
	db         *gorm.DB
	scriptPath string
}

// NewDBProgressStore 创建数据库进度存储
func NewDBProgressStore(db *gorm.DB, scriptPath string) *DBProgressStore {
	return &DBProgressStore{
		db:         db,
		scriptPath: scriptPath,
	}
}

// Load 从数据库加载上次迁移断点
func (s *DBProgressStore) Load(ctx context.Context) (int, error) {
	var progress MigrationProgress
	err := s.db.WithContext(ctx).Where("script_path = ?", s.scriptPath).Order("id DESC").First(&progress).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return 0, nil
		}
		return 0, wrapError("db.First", err)
	}
	return progress.LineNum, nil
}

// Save 保存当前迁移进度到数据库
func (s *DBProgressStore) Save(ctx context.Context, lineNum int) error {
	progress := MigrationProgress{
		ScriptPath: s.scriptPath,
		LineNum:    lineNum,
		UpdatedAt:  time.Now(),
	}

	err := s.db.WithContext(ctx).Create(&progress).Error
	if err != nil {
		return wrapError("db.Create", err)
	}
	return nil
}

// NewFileProgressStore 创建文件进度存储
func NewFileProgressStore(path string) *FileProgressStore {
	return &FileProgressStore{
		path:       path,
		timeFormat: TimeFormat,
	}
}

// Load 加载上次迁移断点
func (s *FileProgressStore) Load(_ context.Context) (int, error) {
	file, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, wrapError("os.Open", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lastLine := ""

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lastLine = line
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, wrapError("scanner.Err", err)
	}

	if lastLine == "" {
		return 0, nil
	}

	re := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\t(\d+)$`)
	matches := re.FindStringSubmatch(lastLine)

	if len(matches) < 3 {
		return 0, fmt.Errorf("invalid format in progress file: %s", lastLine)
	}

	var lineNum int
	_, err = fmt.Sscanf(matches[2], "%d", &lineNum)
	if err != nil {
		return 0, wrapError("Sscanf", err)
	}

	return lineNum, nil
}

// Save 保存当前迁移进度
func (s *FileProgressStore) Save(_ context.Context, lineNum int) error {
	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return wrapError("os.OpenFile", err)
	}
	defer file.Close()

	now := time.Now().Format(s.timeFormat)
	line := fmt.Sprintf("%s\t%d\n", now, lineNum)

	_, err = file.WriteString(line)
	if err != nil {
		return wrapError("WriteString", err)
	}

	return nil
}

// Migrator 数据库迁移器
type Migrator struct {
	db            *gorm.DB
	scriptReader  ScriptReader
	progressStore ProgressStore
	logger        logger.Logger
}

// MigratorOption 配置选项
type MigratorOption func(*Migrator)

// WithLogger 设置日志器
func WithLogger(l logger.Logger) MigratorOption {
	return func(m *Migrator) {
		m.logger = l
	}
}

// WithScriptReader 设置脚本读取器
func WithScriptReader(r ScriptReader) MigratorOption {
	return func(m *Migrator) {
		m.scriptReader = r
	}
}

// WithProgressStore 设置进度存储
func WithProgressStore(s ProgressStore) MigratorOption {
	return func(m *Migrator) {
		m.progressStore = s
	}
}

// NewMigrator 创建一个新的数据库迁移器
func NewMigrator(db *gorm.DB, scriptPath string, opts ...MigratorOption) *Migrator {
	m := &Migrator{
		db:            db,
		scriptReader:  NewFileScriptReader(scriptPath),
		progressStore: NewDBProgressStore(db, scriptPath),
		logger:        logger.Nop(),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Migrate 执行迁移
func (m *Migrator) Migrate(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 确保迁移进度表存在
	if err := m.db.WithContext(ctx).AutoMigrate(&MigrationProgress{}); err != nil {
		return wrapError("AutoMigrate", err)
	}

	statements, err := m.parseSQLStatements(ctx)
	if err != nil {
		return wrapError("parseSQLStatements", err)
	}

	if len(statements) == 0 {
		m.logger.Info("No migrations to apply")
		return nil
	}

	for _, stmt := range statements {
		m.logger.Debug("Executing SQL", "sql", stmt.SQL, "line_number", stmt.LineNum)

		if stmt.SQL != "" {
			if err := m.db.WithContext(ctx).Exec(stmt.SQL).Error; err != nil {
				return wrapError("db.Exec", err)
			}
		}

		if err := m.progressStore.Save(ctx, stmt.LineNum); err != nil {
			return wrapError("recordProgress", err)
		}
	}

	return nil
}

// parseSQLStatements 解析 SQL 文件并提取语句
func (m *Migrator) parseSQLStatements(ctx context.Context) ([]SQLStatement, error) {
	lastLine, err := m.progressStore.Load(ctx)
	if err != nil {
		return nil, err
	}

	content, err := m.scriptReader.Read()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(content, "\n")
	lineNumber := 0
	var result []SQLStatement
	var builder strings.Builder

	for _, line := range lines {
		lineNumber++
		if lineNumber <= lastLine {
			continue
		}

		content := strings.TrimSpace(line)

		switch {
		case content == MigrationMarker:
			sql := builder.String()
			if sql != "" {
				result = append(result, SQLStatement{
					LineNum: lineNumber,
					SQL:     sql,
				})
			}
			builder.Reset()

		case content == "":
			continue

		case strings.HasPrefix(content, "--"):
			continue

		default:
			builder.WriteString(content)
			builder.WriteByte('\n')
		}
	}

	if builder.Len() > 0 {
		result = append(result, SQLStatement{
			LineNum: lineNumber,
			SQL:     builder.String(),
		})
	}

	return result, nil
}
