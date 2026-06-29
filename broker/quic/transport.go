package quic

import (
	"encoding/binary"
	"io"

	"github.com/quic-go/quic-go"
)

const defaultMaxMessageSize = 10 * 1024 * 1024

// Transport QUIC 传输
type Transport struct {
	stream         *quic.Stream
	addr           string
	maxMessageSize int
}

// NewTransport 创建一个新的 QUIC 传输
func NewTransport(stream *quic.Stream, addr string, maxMessageSize ...int) *Transport {
	size := defaultMaxMessageSize
	if len(maxMessageSize) > 0 && maxMessageSize[0] > 0 {
		size = maxMessageSize[0]
	}
	return &Transport{stream: stream, addr: addr, maxMessageSize: size}
}

// ReadMessage 读取消息
func (t *Transport) ReadMessage() ([]byte, error) {
	var length uint32
	if err := binary.Read(t.stream, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length > uint32(t.maxMessageSize) {
		return nil, io.ErrUnexpectedEOF
	}
	data := make([]byte, length)
	_, err := io.ReadFull(t.stream, data)
	return data, err
}

// WriteMessage 写入消息
func (t *Transport) WriteMessage(data []byte) error {
	if err := binary.Write(t.stream, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := t.stream.Write(data)
	return err
}

// Close 关闭传输层
func (t *Transport) Close() error {
	return t.stream.Close()
}

// RemoteAddr 获取远程地址
func (t *Transport) RemoteAddr() string {
	return t.addr
}
