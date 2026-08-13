package job

import "context"

// Handler 作业处理器接口
type Handler interface {
	// Execute 执行作业，返回 error 表示执行失败。
	//
	// 执行结果通过修改 job.Result 返回；
	// 执行进度通过 ReportProgress(ctx, progress) 上报。
	// ctx 在作业被取消、超时或执行器停止时会被关闭，处理器应尊重 ctx 的取消。
	Execute(ctx context.Context, job *Job) error
}

// HandlerFunc 函数类型适配 Handler 接口
type HandlerFunc func(ctx context.Context, job *Job) error

// Execute 实现 Handler 接口
func (f HandlerFunc) Execute(ctx context.Context, job *Job) error {
	return f(ctx, job)
}

// progressReporter 进度上报函数
type progressReporter func(progress int)

type progressKey struct{}

// withProgress 将进度上报函数注入上下文
func withProgress(ctx context.Context, report progressReporter) context.Context {
	return context.WithValue(ctx, progressKey{}, report)
}

// ReportProgress 从上下文中上报作业执行进度（0-100），超出范围会被截断。
// 只有在执行器创建的上下文中调用才生效，否则为无操作。
func ReportProgress(ctx context.Context, progress int) {
	if report, ok := ctx.Value(progressKey{}).(progressReporter); ok {
		if progress < 0 {
			progress = 0
		}
		if progress > 100 {
			progress = 100
		}
		report(progress)
	}
}
