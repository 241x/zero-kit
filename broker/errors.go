package broker

import "errors"

var (
	// ErrPeerNotFound 连接未找到
	ErrPeerNotFound = errors.New("peer not found")

	// ErrRoomNotFound 房间未找到
	ErrRoomNotFound = errors.New("room not found")

	// ErrBrokerClosed 代理已关闭
	ErrBrokerClosed = errors.New("broker is closed")

	// ErrSendBufferFull 发送缓冲区已满
	ErrSendBufferFull = errors.New("send buffer full")

	// ErrInvalidPeer 无效的连接
	ErrInvalidPeer = errors.New("invalid peer: not created by this broker")

	// ErrPeerClosed 连接已关闭
	ErrPeerClosed = errors.New("peer is closed")
)
