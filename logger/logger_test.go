package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/241x/zero-kit/logger"
)

// TestNew_WithLevel 测试 WithLevel 选项
func TestNew_WithLevel(t *testing.T) {
	tests := []struct {
		name  string
		level logger.Level
	}{
		{"Debug 级别", logger.DebugLevel},
		{"Info 级别", logger.InfoLevel},
		{"Warn 级别", logger.WarnLevel},
		{"Error 级别", logger.ErrorLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := logger.New(
				logger.WithLevel(tt.level),
				logger.WithWriter(&buf),
			)

			assert.NotNil(t, log)
			log.Info("test message")
		})
	}
}

// TestNew_WithWriter 测试 WithWriter 选项
func TestNew_WithWriter(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf),
	)

	log.Info("test message")

	assert.True(t, buf.Len() > 0, "期望有日志输出")
}

// TestNew_MultipleWriters 测试多 Writer 输出
func TestNew_MultipleWriters(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf1),
		logger.WithWriter(&buf2),
	)

	log.Info("multi-writer test")

	assert.True(t, buf1.Len() > 0, "buf1 期望有输出")
	assert.True(t, buf2.Len() > 0, "buf2 期望有输出")
	assert.Equal(t, buf1.String(), buf2.String(), "两个 Writer 输出应该相同")
}

// TestNew_NoOptions 无选项时返回 Nop
func TestNew_NoOptions(t *testing.T) {
	log := logger.New()

	// 无选项时应该返回 Nop Logger，可以安全调用
	assert.NotPanics(t, func() {
		log.Info("test")
	})
}

// TestNop 测试 Nop Logger
func TestNop(t *testing.T) {
	log := logger.Nop()

	assert.NotNil(t, log)

	// Nop Logger 应该可以安全调用所有方法而不 panic
	assert.NotPanics(t, func() {
		log.Info("test")
		log.Debug("test")
		log.Warn("test")
		log.Error("test")
		log.Err(nil, "test")
	})
}

// TestLogLevel_Debug 测试 Debug 级别日志
func TestLogLevel_Debug(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.DebugLevel),
		logger.WithWriter(&buf),
	)

	log.Debug("debug message")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)
	assert.Equal(t, "debug", entry["level"])
	assert.Equal(t, "debug message", entry["message"])
}

// TestLogLevel_Info 测试 Info 级别日志
func TestLogLevel_Info(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf),
	)

	log.Info("info message")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)
	assert.Equal(t, "info", entry["level"])
	assert.Equal(t, "info message", entry["message"])
}

// TestLogLevel_Warn 测试 Warn 级别日志
func TestLogLevel_Warn(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.WarnLevel),
		logger.WithWriter(&buf),
	)

	log.Warn("warn message")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)
	assert.Equal(t, "warn", entry["level"])
	assert.Equal(t, "warn message", entry["message"])
}

// TestLogLevel_Error 测试 Error 级别日志
func TestLogLevel_Error(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.ErrorLevel),
		logger.WithWriter(&buf),
	)

	log.Error("error message")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)
	assert.Equal(t, "error", entry["level"])
	assert.Equal(t, "error message", entry["message"])
}

// TestLogLevel_Filtering 测试日志级别过滤
func TestLogLevel_Filtering(t *testing.T) {
	tests := []struct {
		name         string
		level        logger.Level
		logFunc      func(logger.Logger)
		shouldOutput bool
	}{
		{"Info 级别过滤 Debug", logger.InfoLevel, func(l logger.Logger) { l.Debug("debug") }, false},
		{"Warn 级别过滤 Debug", logger.WarnLevel, func(l logger.Logger) { l.Debug("debug") }, false},
		{"Warn 级别过滤 Info", logger.WarnLevel, func(l logger.Logger) { l.Info("info") }, false},
		{"Error 级别过滤 Warn", logger.ErrorLevel, func(l logger.Logger) { l.Warn("warn") }, false},
		{"Info 级别输出 Info", logger.InfoLevel, func(l logger.Logger) { l.Info("info") }, true},
		{"Debug 级别输出所有", logger.DebugLevel, func(l logger.Logger) { l.Debug("debug") }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := logger.New(
				logger.WithLevel(tt.level),
				logger.WithWriter(&buf),
			)

			tt.logFunc(log)

			if tt.shouldOutput {
				assert.True(t, buf.Len() > 0, "期望有输出")
			} else {
				assert.Equal(t, 0, buf.Len(), "期望无输出")
			}
		})
	}
}

// TestLogWithFields 测试带字段的日志输出
func TestLogWithFields(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf),
	)

	log.Info("test message", "key1", "value1", "key2", 123, "key3", true)

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)

	assert.Equal(t, "value1", entry["key1"])
	assert.Equal(t, float64(123), entry["key2"]) // JSON 数字解析为 float64
	assert.Equal(t, true, entry["key3"])
}

// TestErr 测试 Err 方法
func TestErr(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.ErrorLevel),
		logger.WithWriter(&buf),
	)

	testErr := assert.AnError
	log.Err(testErr, "error occurred", "code", 500)

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)

	assert.Equal(t, "error", entry["level"])
	assert.Equal(t, "error occurred", entry["message"])
	assert.Contains(t, entry, "error")
}

// TestWith 测试 With 方法创建带字段的 Logger
func TestWith(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf),
	)

	// 创建带预设字段的新 Logger
	logWithFields := log.With("service", "test-service", "version", "1.0.0")
	logWithFields.Info("message with fields")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)

	assert.Equal(t, "test-service", entry["service"])
	assert.Equal(t, "1.0.0", entry["version"])
	assert.Equal(t, "message with fields", entry["message"])

	// 验证返回的是 Logger 接口类型
	var _ logger.Logger = logWithFields
}

// TestWith_Chained 测试 With 链式调用
func TestWith_Chained(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf),
	)

	log1 := log.With("key1", "value1")
	log2 := log1.With("key2", "value2")
	log2.Info("chained message")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)

	assert.Equal(t, "value1", entry["key1"])
	assert.Equal(t, "value2", entry["key2"])
}

// TestWithContext 测试 WithContext 方法
func TestWithContext(t *testing.T) {
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&bytes.Buffer{}),
	)

	t.Run("将 Logger 存入 context", func(t *testing.T) {
		ctx := context.Background()
		ctx = log.WithContext(ctx)

		retrieved := logger.Ctx(ctx)
		assert.Equal(t, log, retrieved)
	})

	t.Run("从空 context 获取返回 Nop", func(t *testing.T) {
		ctx := context.Background()
		retrieved := logger.Ctx(ctx)
		assert.Equal(t, logger.Nop(), retrieved)
	})

	t.Run("不覆盖已存在的 Logger", func(t *testing.T) {
		ctx := context.Background()
		ctx = log.WithContext(ctx)

		anotherLog := logger.New(logger.WithLevel(logger.DebugLevel))
		ctx = anotherLog.WithContext(ctx)

		retrieved := logger.Ctx(ctx)
		assert.Equal(t, log, retrieved, "应该保留第一个 Logger")
	})
}

// TestConsoleWriter 测试控制台输出配置
func TestConsoleWriter(t *testing.T) {
	// ConsoleWriter 输出到 stderr，验证不会 panic
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithConsole(),
	)

	assert.NotNil(t, log)
	assert.NotPanics(t, func() {
		log.Info("console test")
	})
}

// TestFileWriter 测试文件输出配置
func TestFileWriter(t *testing.T) {
	// WithFile 会尝试创建 runtime/logs/app.log
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithFile(),
	)

	assert.NotNil(t, log)
}

// TestCallerInfo 测试调用者信息
func TestCallerInfo(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf),
	)

	log.Info("test caller")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)

	assert.Contains(t, entry, "caller")
	caller := entry["caller"].(string)
	assert.True(t, strings.Contains(caller, "logger_test.go"))
}

// TestTimestamp 测试时间戳
func TestTimestamp(t *testing.T) {
	var buf bytes.Buffer
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&buf),
	)

	log.Info("test timestamp")

	var entry map[string]any
	err := json.Unmarshal(buf.Bytes(), &entry)
	assert.NoError(t, err)

	assert.Contains(t, entry, "time")
	timeStr := entry["time"].(string)
	assert.NotEmpty(t, timeStr)
}

// TestLoggerInterface 验证 Logger 实现了接口
func TestLoggerInterface(t *testing.T) {
	log := logger.New(
		logger.WithLevel(logger.InfoLevel),
		logger.WithWriter(&bytes.Buffer{}),
	)

	// 编译期检查：确保 Logger 实现了 logger.Logger 接口
	var _ logger.Logger = log
	var _ logger.Logger = logger.Nop()
}
