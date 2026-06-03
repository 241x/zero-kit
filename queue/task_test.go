package queue_test

import (
	"testing"
	"time"

	"github.com/241x/zero-kit/queue"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewTask 验证创建的任务具有正确的字段值和默认值
func TestNewTask(t *testing.T) {
	task := queue.NewTask("test-queue", "test-type", []byte("hello"))

	assert.NotEmpty(t, task.ID)
	assert.Len(t, task.ID, 36)
	assert.Equal(t, "test-queue", task.Queue)
	assert.Equal(t, "test-type", task.Type)
	assert.Equal(t, []byte("hello"), task.Payload)
	assert.Equal(t, 3, task.MaxRetries)
	assert.Equal(t, 0, task.RetryCount)
	assert.Equal(t, queue.TaskStatusPending, task.Status)
	assert.Equal(t, int64(0), task.DelayUntil)
	assert.Equal(t, int64(0), task.StartedAt)
	assert.Equal(t, int64(0), task.CompletedAt)
	assert.Equal(t, "", task.Error)

	require.NotNil(t, task.Metadata)
	assert.Empty(t, task.Metadata)

	now := time.Now().Unix()
	assert.InDelta(t, now, task.CreatedAt, 2)
}

// TestNewTask_NilPayload 验证 payload 为 nil 时不会 panic
func TestNewTask_NilPayload(t *testing.T) {
	task := queue.NewTask("q", "t", nil)
	assert.Nil(t, task.Payload)
}

// TestNewTask_EmptyPayload 验证 payload 为空切片时正常工作
func TestNewTask_EmptyPayload(t *testing.T) {
	task := queue.NewTask("q", "t", []byte{})
	assert.Empty(t, task.Payload)
}

// TestNewTask_UniqueIDs 验证连续创建 1000 个任务的 ID 不会重复
func TestNewTask_UniqueIDs(t *testing.T) {
	ids := make(map[string]struct{})
	for range 1000 {
		task := queue.NewTask("q", "t", []byte("payload"))
		_, exists := ids[task.ID]
		assert.False(t, exists, "duplicate task ID found: %s", task.ID)
		ids[task.ID] = struct{}{}
	}
}

// TestTaskWithDelay 验证延迟设置正确且返回自身引用
func TestTaskWithDelay(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	result := task.WithDelay(5 * time.Second)

	assert.Same(t, task, result)

	now := time.Now().Unix()
	assert.InDelta(t, now+5, task.DelayUntil, 2)
}

// TestTaskWithDelay_Zero 验证零延迟时 DelayUntil 等于当前时间
func TestTaskWithDelay_Zero(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	task.WithDelay(0)
	assert.InDelta(t, time.Now().Unix(), task.DelayUntil, 2)
}

// TestTaskWithMaxRetries 验证最大重试次数设置正确且返回自身引用
func TestTaskWithMaxRetries(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	result := task.WithMaxRetries(10)

	assert.Same(t, task, result)
	assert.Equal(t, 10, task.MaxRetries)
}

// TestTaskWithMaxRetries_Zero 验证零次重试时不会产生异常
func TestTaskWithMaxRetries_Zero(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	task.WithMaxRetries(0)
	assert.Equal(t, 0, task.MaxRetries)
}

// TestTaskWithMetadata 验证元数据写入正确且返回自身引用
func TestTaskWithMetadata(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	result := task.WithMetadata("key1", "value1")

	assert.Same(t, task, result)
	assert.Equal(t, "value1", task.Metadata["key1"])
}

// TestTaskWithMetadata_MultipleKeys 验证多个元数据键值对互不覆盖
func TestTaskWithMetadata_MultipleKeys(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	task.WithMetadata("key1", "value1")
	task.WithMetadata("key2", "value2")
	task.WithMetadata("key3", "value3")

	assert.Equal(t, "value1", task.Metadata["key1"])
	assert.Equal(t, "value2", task.Metadata["key2"])
	assert.Equal(t, "value3", task.Metadata["key3"])
	assert.Len(t, task.Metadata, 3)
}

// TestTaskWithMetadata_Overwrite 验证相同 key 的值会被覆盖
func TestTaskWithMetadata_Overwrite(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	task.WithMetadata("key", "old")
	task.WithMetadata("key", "new")

	assert.Equal(t, "new", task.Metadata["key"])
	assert.Len(t, task.Metadata, 1)
}

// TestTaskWithMetadata_NilMap 验证 Metadata 为 nil 时自动初始化
func TestTaskWithMetadata_NilMap(t *testing.T) {
	task := &queue.Task{Metadata: nil}
	task.WithMetadata("key", "value")

	assert.NotNil(t, task.Metadata)
	assert.Equal(t, "value", task.Metadata["key"])
}

// TestTaskWithMetadata_EmptyValue 验证空字符串值也能正常存储
func TestTaskWithMetadata_EmptyValue(t *testing.T) {
	task := queue.NewTask("q", "t", []byte("payload"))
	task.WithMetadata("key", "")

	assert.Contains(t, task.Metadata, "key")
	assert.Equal(t, "", task.Metadata["key"])
}

// TestTaskMarshalUnmarshal 验证完整任务的序列化→反序列化往返一致性
func TestTaskMarshalUnmarshal(t *testing.T) {
	original := queue.NewTask("test-queue", "test-type", []byte("test payload"))
	original.WithMetadata("trace_id", "abc-123")
	original.WithMetadata("user_id", "456")
	original.WithDelay(10 * time.Second)

	data, err := original.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := queue.UnmarshalTask(data)
	require.NoError(t, err)

	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.Queue, decoded.Queue)
	assert.Equal(t, original.Type, decoded.Type)
	assert.Equal(t, original.Payload, decoded.Payload)
	assert.Equal(t, original.MaxRetries, decoded.MaxRetries)
	assert.Equal(t, original.RetryCount, decoded.RetryCount)
	assert.Equal(t, original.DelayUntil, decoded.DelayUntil)
	assert.Equal(t, original.CreatedAt, decoded.CreatedAt)
	assert.Equal(t, original.StartedAt, decoded.StartedAt)
	assert.Equal(t, original.CompletedAt, decoded.CompletedAt)
	assert.Equal(t, original.Status, decoded.Status)
	assert.Equal(t, original.Error, decoded.Error)
	assert.Equal(t, original.Metadata["trace_id"], decoded.Metadata["trace_id"])
	assert.Equal(t, original.Metadata["user_id"], decoded.Metadata["user_id"])
}

// TestTaskMarshalUnmarshal_EmptyMetadata 验证空 map 序列化后反序列化仍为非 nil 空 map
func TestTaskMarshalUnmarshal_EmptyMetadata(t *testing.T) {
	original := queue.NewTask("q", "t", []byte("payload"))

	data, err := original.Marshal()
	require.NoError(t, err)

	decoded, err := queue.UnmarshalTask(data)
	require.NoError(t, err)

	assert.NotNil(t, decoded.Metadata)
	assert.Empty(t, decoded.Metadata)
}

// TestTaskMarshalUnmarshal_NilPayload 验证 nil payload 的往返一致性
func TestTaskMarshalUnmarshal_NilPayload(t *testing.T) {
	original := queue.NewTask("q", "t", nil)

	data, err := original.Marshal()
	require.NoError(t, err)

	decoded, err := queue.UnmarshalTask(data)
	require.NoError(t, err)

	assert.Nil(t, decoded.Payload)
}

// TestTaskMarshalUnmarshal_AllStatuses 验证所有任务状态的序列化往返一致性
func TestTaskMarshalUnmarshal_AllStatuses(t *testing.T) {
	statuses := []queue.TaskStatus{
		queue.TaskStatusPending,
		queue.TaskStatusProcessing,
		queue.TaskStatusCompleted,
		queue.TaskStatusFailed,
		queue.TaskStatusDeadLetter,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			task := queue.NewTask("q", "t", []byte("payload"))
			task.Status = status

			data, err := task.Marshal()
			require.NoError(t, err)

			decoded, err := queue.UnmarshalTask(data)
			require.NoError(t, err)
			assert.Equal(t, status, decoded.Status)
		})
	}
}

// TestUnmarshalTask_InvalidJSON 验证非法 JSON 输入返回错误
func TestUnmarshalTask_InvalidJSON(t *testing.T) {
	_, err := queue.UnmarshalTask([]byte("not json"))
	assert.Error(t, err)
}

// TestUnmarshalTask_EmptyData 验证空数据输入返回错误
func TestUnmarshalTask_EmptyData(t *testing.T) {
	_, err := queue.UnmarshalTask([]byte{})
	assert.Error(t, err)
}

// TestUnmarshalTask_PartialJSON 验证部分 JSON 输入时缺失字段使用零值
func TestUnmarshalTask_PartialJSON(t *testing.T) {
	data := []byte(`{"id": "test-id-123"}`)
	task, err := queue.UnmarshalTask(data)
	require.NoError(t, err)

	assert.Equal(t, "test-id-123", task.ID)
	assert.Equal(t, queue.TaskStatus(""), task.Status)
	assert.Equal(t, 0, task.MaxRetries)
	assert.Nil(t, task.Payload)
}

// TestTaskChainedCalls 验证多个 With 方法的链式调用结果正确
func TestTaskChainedCalls(t *testing.T) {
	task := queue.NewTask("email-queue", "send-email", []byte(`{"to":"user@example.com"}`)).
		WithMaxRetries(5).
		WithDelay(30*time.Second).
		WithMetadata("priority", "high").
		WithMetadata("source", "api")

	assert.Equal(t, "email-queue", task.Queue)
	assert.Equal(t, "send-email", task.Type)
	assert.Equal(t, 5, task.MaxRetries)
	assert.NotEqual(t, int64(0), task.DelayUntil)
	assert.Equal(t, "high", task.Metadata["priority"])
	assert.Equal(t, "api", task.Metadata["source"])
}
