package logger

import "context"

// Level 日志级别
type Level int

const (
	// DebugLevel 日志 debug 级别
	DebugLevel Level = iota
	// InfoLevel 日志 info 级别
	InfoLevel
	// WarnLevel 日志 warn 级别
	WarnLevel
	// ErrorLevel 日志 error 级别
	ErrorLevel
	// Disabled 日志禁用级别
	Disabled
)

// Logger 日志接口
type Logger interface {
	// Info 记录 Info 级别日志
	Info(msg string, fields ...any)

	// Error 记录 Error 级别日志
	Error(msg string, fields ...any)

	// Debug 记录 Debug 级别日志
	Debug(msg string, fields ...any)

	// Warn 记录 Warn 级别日志
	Warn(msg string, fields ...any)

	// Err 封装 Error 错误
	Err(err error, msg string, fields ...any)

	// Log 指定级别记录日志
	Log(level Level, msg string, fields ...any)

	// WithContext 将日志实例添加到上下文中
	WithContext(ctx context.Context) context.Context

	// With 创建新的 Logger
	With(fields ...any) Logger
}
