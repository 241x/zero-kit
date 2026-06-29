package broker

import (
	"context"
	"sync"

	"github.com/241x/zero-kit/logger"
)

// Peer 代表一个连接
type Peer interface {
	ID() string
	RoomID() string
	RemoteAddr() string
	Metadata() map[string]any
	SetMetadata(key string, value any)
	Send(msg *Message) error
	Close() error
}

// peer 代表一个连接
type peer struct {
	id             string
	roomID         string
	addr           string
	transport      Transport
	sendCh         chan *Message
	metadata       map[string]any
	mu             sync.RWMutex
	broker         *Broker
	logger         logger.Logger
	maxMessageSize int
	closed         bool
}

// newPeer 创建一个新的连接
func newPeer(id string, transport Transport, opts Options) *peer {
	return &peer{
		id:             id,
		addr:           transport.RemoteAddr(),
		transport:      transport,
		sendCh:         make(chan *Message, opts.SendBufSize),
		metadata:       make(map[string]any),
		logger:         opts.Logger,
		maxMessageSize: opts.MaxMessageSize,
	}
}

// ID 获取连接 ID
func (p *peer) ID() string {
	return p.id
}

// RemoteAddr 获取连接远程地址
func (p *peer) RemoteAddr() string {
	return p.addr
}

// RoomID 获取连接房间 ID
func (p *peer) RoomID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.roomID
}

// setRoomID 设置连接房间 ID
func (p *peer) setRoomID(rid string) {
	p.mu.Lock()
	p.roomID = rid
	p.mu.Unlock()
}

// Metadata 获取连接元数据
func (p *peer) Metadata() map[string]any {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cp := make(map[string]any, len(p.metadata))
	for k, v := range p.metadata {
		cp[k] = v
	}
	return cp
}

// SetMetadata 设置连接元数据
func (p *peer) SetMetadata(key string, value any) {
	p.mu.Lock()
	p.metadata[key] = value
	p.mu.Unlock()
}

// Send 发送消息
func (p *peer) Send(msg *Message) error {
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()
	if closed {
		return ErrPeerClosed
	}
	select {
	case p.sendCh <- msg:
		return nil
	default:
		return ErrSendBufferFull
	}
}

// Close 关闭连接
func (p *peer) Close() error {
	return p.transport.Close()
}

// readLoop 读取连接数据的循环
func (p *peer) readLoop(ctx context.Context) {
	defer func() {
		select {
		case <-ctx.Done():
		default:
			p.broker.unregister <- p
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		data, err := p.transport.ReadMessage()
		if err != nil {
			p.logger.Debug("peer read error", "peer_id", p.id, "error", err)
			return
		}

		if len(data) > p.maxMessageSize {
			p.logger.Debug("peer message too large", "peer_id", p.id, "size", len(data))
			continue
		}

		msg, err := UnmarshalMessage(data)
		if err != nil {
			p.logger.Debug("peer unmarshal error", "peer_id", p.id, "error", err)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case p.broker.receive <- &receiveMsg{from: p, msg: msg}:
		}
	}
}

// writeLoop 写入连接数据的循环
func (p *peer) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-p.sendCh:
			if !ok {
				return
			}
			if err := p.transport.WriteMessage(msg.Marshal()); err != nil {
				p.logger.Debug("peer write error", "peer_id", p.id, "error", err)
				return
			}
		}
	}
}

// shutdown 关闭连接
func (p *peer) shutdown() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.sendCh)
	p.mu.Unlock()
}
