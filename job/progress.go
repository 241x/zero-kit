package job

import "context"

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
