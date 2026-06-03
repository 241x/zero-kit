package mysql

import "context"

// traceIDKey 上下文传递请求链路ID
type traceIDKey struct{}

// WithTraceID 注入 traceID
func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, id)
}

// TraceID 读取 traceID
func TraceID(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey{}).(string); ok {
		return v
	}
	return ""
}
