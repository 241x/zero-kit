package broker

import "github.com/241x/zero-kit/logger"

const (
	defaultSendBufSize    = 256
	defaultChannelBufSize = 64
	defaultMaxMessageSize = 10 * 1024 * 1024
)

// Options 代理选项
type Options struct {
	Logger          logger.Logger
	SendBufSize     int
	ChannelBufSize  int
	MaxMessageSize  int
	PeerIDGenerator func() string
	RoomAutoCleanup bool
}

// defaultOptions 默认选项
func defaultOptions() Options {
	return Options{
		Logger:          logger.Nop(),
		SendBufSize:     defaultSendBufSize,
		ChannelBufSize:  defaultChannelBufSize,
		MaxMessageSize:  defaultMaxMessageSize,
		PeerIDGenerator: defaultPeerIDGenerator,
		RoomAutoCleanup: true,
	}
}

// Option 选项
type Option func(*Options)

// WithLogger 使用自定义的日志记录器
func WithLogger(l logger.Logger) Option {
	return func(o *Options) {
		o.Logger = l
	}
}

// WithSendBufSize 使用自定义的发送缓冲区大小
func WithSendBufSize(size int) Option {
	return func(o *Options) {
		o.SendBufSize = size
	}
}

// WithChannelBufSize 使用自定义的通道缓冲区大小
func WithChannelBufSize(size int) Option {
	return func(o *Options) {
		o.ChannelBufSize = size
	}
}

// WithMaxMessageSize 使用自定义的最大消息大小
func WithMaxMessageSize(size int) Option {
	return func(o *Options) {
		o.MaxMessageSize = size
	}
}

// WithPeerIDGenerator 使用自定义的连接 ID 生成器
func WithPeerIDGenerator(gen func() string) Option {
	return func(o *Options) {
		o.PeerIDGenerator = gen
	}
}

// WithRoomAutoCleanup 使用自定义的房间自动清理设置
func WithRoomAutoCleanup(enabled bool) Option {
	return func(o *Options) {
		o.RoomAutoCleanup = enabled
	}
}
