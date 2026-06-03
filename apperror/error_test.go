package apperror_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/241x/zero-kit/apperror"
)

// 测试用错误码
var (
	testUserNotFound = apperror.NewCode(404001, "USER.NOT_FOUND", "用户不存在")
	testSystemError  = apperror.NewCode(500001, "SYSTEM.ERROR", "系统异常，请稍后重试")
)

func TestCode_Value(t *testing.T) {
	if got := testUserNotFound.Value(); got != 404001 {
		t.Errorf("Value() = %d, want 404001", got)
	}
}

func TestCode_String(t *testing.T) {
	if got := testUserNotFound.String(); got != "USER.NOT_FOUND" {
		t.Errorf("String() = %q, want %q", got, "USER.NOT_FOUND")
	}
}

func TestCode_Template(t *testing.T) {
	if got := testUserNotFound.Template(); got != "用户不存在" {
		t.Errorf("Template() = %q, want %q", got, "用户不存在")
	}
}

func TestNew_Basic(t *testing.T) {
	err := apperror.New(testUserNotFound)

	if err.Error() != "用户不存在" {
		t.Errorf("Error() = %q, want %q", err.Error(), "用户不存在")
	}
	if err.Code() != testUserNotFound {
		t.Errorf("Code() mismatch")
	}
	if err.Cause() != nil {
		t.Errorf("Cause() should be nil")
	}
}

func TestNew_WithCause(t *testing.T) {
	cause := errors.New("sql: no rows")
	err := apperror.New(testSystemError, apperror.WithCause(cause))

	if err.Cause() != cause {
		t.Errorf("Cause() = %v, want %v", err.Cause(), cause)
	}
	if err.Unwrap() != cause {
		t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), cause)
	}
}

func TestNew_WithMsg(t *testing.T) {
	err := apperror.New(testUserNotFound, apperror.WithMsg("自定义消息"))

	if err.Error() != "自定义消息" {
		t.Errorf("Error() = %q, want %q", err.Error(), "自定义消息")
	}
}

func TestNew_WithCauseAndMsg(t *testing.T) {
	cause := errors.New("file too large")
	err := apperror.New(testSystemError, apperror.WithCause(cause), apperror.WithMsg("文件超限"))

	if err.Cause() != cause {
		t.Errorf("Cause() mismatch")
	}
	if err.Error() != "文件超限" {
		t.Errorf("Error() = %q, want %q", err.Error(), "文件超限")
	}
}

func TestWrap(t *testing.T) {
	cause := errors.New("connection refused")
	err := apperror.Wrap(testSystemError, cause)

	if err.Error() != "系统异常，请稍后重试" {
		t.Errorf("Error() = %q, want %q", err.Error(), "系统异常，请稍后重试")
	}
	if err.Code() != testSystemError {
		t.Errorf("Code() mismatch")
	}
	if err.Cause() != cause {
		t.Errorf("Cause() = %v, want %v", err.Cause(), cause)
	}
}

func TestError_Is_SameCode(t *testing.T) {
	err1 := apperror.New(testUserNotFound)
	err2 := apperror.New(testUserNotFound)

	if !errors.Is(err1, err2) {
		t.Error("errors.Is should return true for same Code")
	}
}

func TestError_Is_DifferentCode(t *testing.T) {
	err1 := apperror.New(testUserNotFound)
	err2 := apperror.New(testSystemError)

	if errors.Is(err1, err2) {
		t.Error("errors.Is should return false for different Code")
	}
}

func TestError_Is_NonAppError(t *testing.T) {
	err := apperror.New(testUserNotFound)
	plain := errors.New("plain error")

	if errors.Is(err, plain) {
		t.Error("errors.Is should return false for non-*Error target")
	}
}

func TestError_AsType(t *testing.T) {
	err := apperror.New(testUserNotFound)

	appErr, ok := errors.AsType[*apperror.Error](err)
	if !ok {
		t.Fatal("errors.AsType should return true")
	}
	if appErr.Code() != testUserNotFound {
		t.Errorf("Code() mismatch")
	}
}

func TestError_AsType_Wrapped(t *testing.T) {
	inner := apperror.New(testUserNotFound)
	wrapped := fmt.Errorf("service layer: %w", inner)

	appErr, ok := errors.AsType[*apperror.Error](wrapped)
	if !ok {
		t.Fatal("errors.AsType should unwrap and find *Error")
	}
	if appErr.Code() != testUserNotFound {
		t.Errorf("Code() mismatch")
	}
}

func TestError_Unwrap_Chain(t *testing.T) {
	root := errors.New("sql: duplicate key")
	dbErr := apperror.Wrap(testSystemError, root)
	svcErr := apperror.New(testSystemError, apperror.WithCause(dbErr))

	// 第一层 unwrap
	if errors.Unwrap(svcErr) != dbErr {
		t.Errorf("first Unwrap should return dbErr")
	}

	// 第二层 unwrap
	if errors.Unwrap(dbErr) != root {
		t.Errorf("second Unwrap should return root")
	}

	// errors.Is 应能穿透整个链
	if !errors.Is(svcErr, dbErr) {
		t.Error("errors.Is should find dbErr in chain")
	}
}

func TestError_Format_VerbV(t *testing.T) {
	err := apperror.New(testUserNotFound)
	got := fmt.Sprintf("%v", err)
	if got != "用户不存在" {
		t.Errorf("%%v = %q, want %q", got, "用户不存在")
	}
}

func TestError_Format_VerbPlusV(t *testing.T) {
	cause := errors.New("db timeout")
	err := apperror.New(testSystemError, apperror.WithCause(cause))

	got := fmt.Sprintf("%+v", err)

	for _, want := range []string{"apperror.Error{", "SYSTEM.ERROR", "500001", "系统异常，请稍后重试", "db timeout"} {
		if !contains(got, want) {
			t.Errorf("%%+v output missing %q\nGot: %s", want, got)
		}
	}
}

func TestError_Format_VerbS(t *testing.T) {
	err := apperror.New(testUserNotFound)
	got := fmt.Sprintf("%s", err)
	if got != "用户不存在" {
		t.Errorf("%%s = %q, want %q", got, "用户不存在")
	}
}

func TestError_Format_VerbQ(t *testing.T) {
	err := apperror.New(testUserNotFound)
	got := fmt.Sprintf("%q", err)
	if got != `"用户不存在"` {
		t.Errorf("%%q = %q, want %q", got, `"用户不存在"`)
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
