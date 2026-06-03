package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/natefinch/lumberjack.v2"
)

// ZeroLogger 日志组件
type ZeroLogger struct {
	logger  zerolog.Logger
	level   Level
	writers []io.Writer
}

// Option 日志组件配置项
type Option func(*ZeroLogger)

// WithLevel 设置日志级别
func WithLevel(level Level) Option { return func(l *ZeroLogger) { l.level = level } }

// WithWriters 设置日志写入器
func WithWriters(w []io.Writer) Option { return func(l *ZeroLogger) { l.writers = w } }

// WithWriter 添加日志写入器
func WithWriter(w io.Writer) Option { return func(l *ZeroLogger) { l.writers = append(l.writers, w) } }

// WithConsole 设置控制台输出
func WithConsole() Option {
	return func(l *ZeroLogger) {
		l.writers = append(l.writers, zerolog.ConsoleWriter{
			Out: os.Stderr,
		})
	}
}

// FileConfig 文件日志配置
type FileConfig struct {
	Path       string
	Filename   string
	MaxSize    int
	MaxAge     int
	MaxBackups int
	Compress   bool
	LocalTime  bool
}

// WithFile 添加文件写入器（使用默认配置）
func WithFile() Option {
	return WithFileWithConfig(FileConfig{
		Path:       "runtime/logs",
		Filename:   "app.log",
		MaxSize:    100,
		MaxAge:     30,
		MaxBackups: 3,
		Compress:   true,
		LocalTime:  true,
	})
}

// WithFileWithConfig 添加文件写入器（使用自定义配置）
func WithFileWithConfig(cfg FileConfig) Option {
	return func(l *ZeroLogger) {
		filename, err := filepath.Abs(filepath.Join(cfg.Path, cfg.Filename))
		if err != nil {
			panic(fmt.Sprintf("logger: failed to get absolute path: %v", err))
		}
		l.writers = append(l.writers, &lumberjack.Logger{
			Filename:   filename,
			MaxSize:    cfg.MaxSize,
			MaxAge:     cfg.MaxAge,
			MaxBackups: cfg.MaxBackups,
			LocalTime:  cfg.LocalTime,
			Compress:   cfg.Compress,
		})
	}
}

// WithMongo 添加MongoDB写入器
func WithMongo(db *mongo.Database) Option {
	return func(l *ZeroLogger) {
		if db != nil {
			l.writers = append(l.writers, &mongoWriter{db: db})
		}
	}
}

// Nop 创建一个空日志组件
func Nop() *ZeroLogger { return &ZeroLogger{logger: zerolog.Nop()} }

// New 创建日志组件
func New(opts ...Option) *ZeroLogger {
	l := &ZeroLogger{
		level:   Disabled,
		writers: []io.Writer{},
	}

	for _, opt := range opts {
		opt(l)
	}

	if l.level == Disabled {
		return Nop()
	}

	if len(l.writers) == 0 {
		return Nop()
	}

	// 初始化日志级别
	logLevel := zerolog.Disabled
	switch l.level {
	case DebugLevel:
		logLevel = zerolog.DebugLevel
	case InfoLevel:
		logLevel = zerolog.InfoLevel
	case WarnLevel:
		logLevel = zerolog.WarnLevel
	case ErrorLevel:
		logLevel = zerolog.ErrorLevel
	}

	multiWriter := io.MultiWriter(l.writers...)
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"
	l.logger = zerolog.New(multiWriter).
		With().
		Caller().
		Timestamp().
		Logger().
		Level(logLevel)

	return l
}

// Info 记录信息日志
func (l *ZeroLogger) Info(msg string, fields ...any) {
	l.logger.Info().CallerSkipFrame(1).Fields(fields).Msg(msg)
}

// Error 记录错误信息日志
func (l *ZeroLogger) Error(msg string, fields ...any) {
	l.logger.Error().CallerSkipFrame(1).Fields(fields).Msg(msg)
}

// Debug 记录调试信息日志
func (l *ZeroLogger) Debug(msg string, fields ...any) {
	l.logger.Debug().CallerSkipFrame(1).Fields(fields).Msg(msg)
}

// Warn 记录警告信息日志
func (l *ZeroLogger) Warn(msg string, fields ...any) {
	l.logger.Warn().CallerSkipFrame(1).Fields(fields).Msg(msg)
}

// Err 记录包含错误信息日志
func (l *ZeroLogger) Err(err error, msg string, fields ...any) {
	l.logger.Err(err).CallerSkipFrame(1).Fields(fields).Msg(msg)
}

// Log 指定级别记录日志
func (l *ZeroLogger) Log(level Level, msg string, fields ...any) {
	switch level {
	case DebugLevel:
		l.Debug(msg, fields...)
	case InfoLevel:
		l.Info(msg, fields...)
	case WarnLevel:
		l.Warn(msg, fields...)
	case ErrorLevel:
		l.Error(msg, fields...)
	}
}

// ctxKey 日志实例的上下文键名
type ctxKey struct{}

// WithContext 将日志实例添加到上下文中
func (l *ZeroLogger) WithContext(ctx context.Context) context.Context {
	if l.level == Disabled {
		return ctx
	}

	// 如果context已经有logger，不覆盖
	if _, ok := ctx.Value(ctxKey{}).(*ZeroLogger); ok {
		return ctx
	}

	return context.WithValue(ctx, ctxKey{}, l)
}

// Ctx 从上下文中获取日志实例
func Ctx(ctx context.Context) *ZeroLogger {
	if ctx == nil {
		return Nop()
	}
	if l, ok := ctx.Value(ctxKey{}).(*ZeroLogger); ok {
		return l
	}
	return Nop()
}

// With 创建一个新的日志实例，并添加字段
func (l ZeroLogger) With(fields ...any) Logger {
	l2 := &ZeroLogger{
		logger:  l.logger.With().Fields(fields).Logger(),
		level:   l.level,
		writers: l.writers,
	}

	return l2
}
