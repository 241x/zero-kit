package apperror

import (
	"fmt"
)

// Error 应用错误，唯一的核心错误类型
type Error struct {
	code  Code
	cause error
	msg   string
}

// Error 实现 error 接口，返回错误消息
func (e *Error) Error() string {
	if e.msg != "" {
		return e.msg
	}
	return e.code.template
}

// Unwrap 实现 errors.Unwrap 协议，返回内部原因
func (e *Error) Unwrap() error {
	return e.cause
}

// Code 返回错误码
func (e *Error) Code() Code {
	return e.code
}

// Cause 返回内部原始错误
func (e *Error) Cause() error {
	return e.cause
}

// Is 实现 errors.Is 协议，按 Code 匹配
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.code == t.code
}

// Format 实现 fmt.Formatter，支持 %+v 打印调试信息
func (e *Error) Format(f fmt.State, verb rune) {
	switch verb {
	case 'v':
		if f.Flag('+') {
			fmt.Fprintf(f, "apperror.Error{\n")
			fmt.Fprintf(f, "  code:    %s (%d)\n", e.code.name, e.code.value)
			fmt.Fprintf(f, "  message: %s\n", e.Error())
			if e.cause != nil {
				fmt.Fprintf(f, "  cause:   %+v\n", e.cause)
			}

			fmt.Fprintf(f, "}")
			return
		}
		fmt.Fprint(f, e.Error())
	case 's':
		fmt.Fprint(f, e.Error())
	case 'q':
		fmt.Fprintf(f, "%q", e.Error())
	}
}

// Option 错误构建选项
type Option func(*Error)

// WithCause 设置内部原始错误
func WithCause(err error) Option {
	return func(e *Error) {
		e.cause = err
	}
}

// WithMsg 覆盖默认消息
func WithMsg(msg string) Option {
	return func(e *Error) {
		e.msg = msg
	}
}

// New 创建应用错误
func New(code Code, opts ...Option) *Error {
	e := &Error{code: code}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Wrap 快捷包装已有错误
func Wrap(code Code, cause error) *Error {
	return New(code, WithCause(cause))
}
