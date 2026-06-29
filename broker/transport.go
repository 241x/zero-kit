package broker

// Transport 代表一个传输层
type Transport interface {
	// ReadMessage 读取消息
	ReadMessage() ([]byte, error)

	// WriteMessage 写入消息
	WriteMessage([]byte) error

	// Close 关闭传输层
	Close() error

	// RemoteAddr 获取远程地址
	RemoteAddr() string
}
