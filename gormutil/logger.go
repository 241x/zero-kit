package gormutil

import (
	"context"
	"errors"
	"time"

	"github.com/241x/zero-kit/logger"

	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Logger 自定义GORM logger实现
type Logger struct {
	logger.Logger
	LogLevel gormlogger.LogLevel
}

// NewLogger 创建新的Logger实例
func NewLogger(l logger.Logger) *Logger {
	return &Logger{
		Logger:   l,
		LogLevel: gormlogger.Info,
	}
}

// buildTraceField 构建traceId字段，空字符串时省略
func buildTraceField(ctx context.Context) []any {
	if traceID := TraceID(ctx); traceID != "" {
		return []any{"traceId", traceID}
	}
	return nil
}

// LogMode 设置日志级别
func (l *Logger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	newLogger := *l
	newLogger.LogLevel = level
	return &newLogger
}

// Info 打印info级别日志
func (l *Logger) Info(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= gormlogger.Info {
		l.Logger.Info(msg, append(buildTraceField(ctx), data...)...)
	}
}

// Warn 打印warn级别日志
func (l *Logger) Warn(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= gormlogger.Warn {
		l.Logger.Warn(msg, append(buildTraceField(ctx), data...)...)
	}
}

// Error 打印error级别日志
func (l *Logger) Error(ctx context.Context, msg string, data ...any) {
	if l.LogLevel >= gormlogger.Error {
		l.Logger.Error(msg, append(buildTraceField(ctx), data...)...)
	}
}

// Trace 打印SQL日志
func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()
	fields := append(buildTraceField(ctx),
		"sql", sql,
		"rows", rows,
		"timeMs", float64(elapsed.Nanoseconds())/1e6,
	)

	switch {
	case err != nil && l.LogLevel >= gormlogger.Error:
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			l.Logger.Error("SQL执行错误", append(fields, "error", err)...)
		}
	case elapsed > time.Second && l.LogLevel >= gormlogger.Warn:
		l.Logger.Warn("慢SQL查询", append(fields, "threshold", "1s")...)
	case l.LogLevel == gormlogger.Info:
		l.Logger.Debug("SQL执行", fields...)
	}
}
