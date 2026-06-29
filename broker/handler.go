package broker

// Handler 处理器
type Handler interface {
	// HandleMessage 处理消息
	HandleMessage(from Peer, msg *Message)

	// HandleDisconnect 处理断开连接
	HandleDisconnect(from Peer)
}
