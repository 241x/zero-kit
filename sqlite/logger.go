package sqlite

import (
	"context"
	"errors"
	"time"

	logger2 "github.com/241x/zero-kit/logger"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Logger 自定义GORM logger实现
type Logger struct {
	logger2.Logger
	LogLevel logger.LogLevel
}

// NewLogger 创建新的Logger实例
func NewLogger(l logger2.Logger) *Logger {
	return &Logger{
		Logger:   l,
		LogLevel: logger.Info,
	}
}

// LogMode 设置日志级别
func (l *Logger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l
	newLogger.LogLevel = level
	return &newLogger
}

// Info 打印info级别日志
func (l *Logger) Info(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= logger.Info {
		l.Logger.Info(msg, append([]any{"traceId", TraceID(ctx)}, data...)...)
	}
}

// Warn 打印warn级别日志
func (l *Logger) Warn(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= logger.Warn {
		l.Logger.Warn(msg, append([]any{"traceId", TraceID(ctx)}, data...)...)
	}
}

// Error 打印error级别日志
func (l *Logger) Error(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= logger.Error {
		l.Logger.Error(msg, append([]any{"traceId", TraceID(ctx)}, data...)...)
	}
}

// Trace 打印SQL日志
func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()
	fields := []any{
		"traceId", TraceID(ctx),
		"sql", sql,
		"rows", rows,
		"timeMs", float64(elapsed.Nanoseconds()) / 1e6,
	}

	switch {
	case err != nil && l.LogLevel >= logger.Error:
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			l.Logger.Error("SQL执行错误", append(fields, "error", err)...)
		}
	case elapsed > time.Second && l.LogLevel >= logger.Warn:
		l.Logger.Warn("慢SQL查询", append(fields, "threshold", "1s")...)
	case l.LogLevel == logger.Info:
		l.Logger.Debug("SQL执行", fields...)
	}
}
