package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/241x/zero-kit/logger"
)

// receiveMsg 接收到的消息
type receiveMsg struct {
	from Peer
	msg  *Message
}

// Broker 中间人
type Broker struct {
	opts    Options
	logger  logger.Logger
	handler Handler

	peers   map[string]*peer
	peersMu sync.RWMutex

	rooms   map[string]*Room
	roomsMu sync.RWMutex

	register   chan *peer
	unregister chan *peer
	receive    chan *receiveMsg

	events *eventBus

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
}

// New 创建一个新的中间人
func New(handler Handler, opts ...Option) *Broker {
	options := defaultOptions()
	for _, o := range opts {
		o(&options)
	}

	return &Broker{
		opts:       options,
		logger:     options.Logger,
		handler:    handler,
		peers:      make(map[string]*peer),
		rooms:      make(map[string]*Room),
		register:   make(chan *peer, options.ChannelBufSize),
		unregister: make(chan *peer, options.ChannelBufSize),
		receive:    make(chan *receiveMsg, options.ChannelBufSize),
		events:     newEventBus(),
	}
}

// Start 启动中间人
func (b *Broker) Start(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return
	}
	b.started = true

	b.ctx, b.cancel = context.WithCancel(ctx)

	b.wg.Add(1)
	go b.run()
}

// Stop 停止中间人
func (b *Broker) Stop() error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	b.cancel()
	b.wg.Wait()

	b.mu.Lock()
	b.started = false
	b.mu.Unlock()

	return nil
}

// Accept 接受一个连接
func (b *Broker) Accept(transport Transport) (Peer, error) {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil, ErrBrokerClosed
	}
	b.mu.Unlock()

	id := b.opts.PeerIDGenerator()
	p := newPeer(id, transport, b.opts)
	p.broker = b

	select {
	case b.register <- p:
	case <-b.ctx.Done():
		return nil, ErrBrokerClosed
	}

	b.wg.Add(2)
	go func() {
		defer b.wg.Done()
		p.readLoop(b.ctx)
	}()
	go func() {
		defer b.wg.Done()
		p.writeLoop(b.ctx)
	}()

	return p, nil
}

// Events 获取事件总线
func (b *Broker) Events() *eventBus {
	return b.events
}

// Peer 获取一个连接
func (b *Broker) Peer(id string) Peer {
	b.peersMu.RLock()
	defer b.peersMu.RUnlock()
	p, ok := b.peers[id]
	if !ok {
		return nil
	}
	return p
}

// Peers 获取所有连接
func (b *Broker) Peers() []Peer {
	b.peersMu.RLock()
	defer b.peersMu.RUnlock()
	peers := make([]Peer, 0, len(b.peers))
	for _, p := range b.peers {
		peers = append(peers, p)
	}
	return peers
}

// Broadcast 广播消息给所有连接
func (b *Broker) Broadcast(msg *Message) {
	b.peersMu.RLock()
	defer b.peersMu.RUnlock()
	for _, p := range b.peers {
		_ = p.Send(msg)
	}
}

// SendTo 发送消息给一个连接
func (b *Broker) SendTo(peerID string, msg *Message) error {
	b.peersMu.RLock()
	p, ok := b.peers[peerID]
	b.peersMu.RUnlock()
	if !ok {
		return ErrPeerNotFound
	}
	return p.Send(msg)
}

// SendToRoom 发送消息给一个房间
func (b *Broker) SendToRoom(roomID string, msg *Message, exclude Peer) {
	b.roomsMu.RLock()
	room := b.rooms[roomID]
	b.roomsMu.RUnlock()
	if room == nil {
		return
	}

	for _, p := range room.Peers() {
		if exclude != nil && p.ID() == exclude.ID() {
			continue
		}
		_ = p.Send(msg)
	}
}

// CreateRoom 创建一个房间
func (b *Broker) CreateRoom() *Room {
	r := newRoom(generateRoomID())
	b.roomsMu.Lock()
	b.rooms[r.id] = r
	b.roomsMu.Unlock()
	return r
}

// Room 获取一个房间
func (b *Broker) Room(id string) *Room {
	b.roomsMu.RLock()
	defer b.roomsMu.RUnlock()
	return b.rooms[id]
}

// JoinRoom 加入一个房间
func (b *Broker) JoinRoom(roomID string, p Peer) error {
	b.roomsMu.RLock()
	room := b.rooms[roomID]
	b.roomsMu.RUnlock()
	if room == nil {
		return ErrRoomNotFound
	}

	peer, ok := p.(*peer)
	if !ok {
		return ErrInvalidPeer
	}
	peer.setRoomID(roomID)
	room.Add(p)

	b.events.Emit(Event{
		Type: EventPeerJoinedRoom,
		Peer: p,
		Room: room,
	})

	return nil
}

// LeaveRoom 离开一个房间
func (b *Broker) LeaveRoom(p Peer) {
	rid := p.RoomID()
	if rid == "" {
		return
	}

	b.roomsMu.RLock()
	room := b.rooms[rid]
	b.roomsMu.RUnlock()
	if room == nil {
		return
	}

	room.Remove(p)
	peer, ok := p.(*peer)
	if !ok {
		b.logger.Warn("LeaveRoom called with invalid peer")
		return
	}
	peer.setRoomID("")

	b.events.Emit(Event{
		Type: EventPeerLeftRoom,
		Peer: p,
		Room: room,
	})

	if b.opts.RoomAutoCleanup && room.Size() == 0 {
		b.roomsMu.Lock()
		delete(b.rooms, rid)
		b.roomsMu.Unlock()
	}
}

// Rooms 获取所有房间
func (b *Broker) Rooms() map[string]*Room {
	b.roomsMu.RLock()
	defer b.roomsMu.RUnlock()
	cp := make(map[string]*Room, len(b.rooms))
	for k, v := range b.rooms {
		cp[k] = v
	}
	return cp
}

// run 运行中间人
func (b *Broker) run() {
	defer b.wg.Done()
	defer b.cleanup()

	for {
		select {
		case <-b.ctx.Done():
			return

		case p := <-b.register:
			b.peersMu.Lock()
			b.peers[p.id] = p
			b.peersMu.Unlock()

			b.logger.Debug("peer connected", "peer_id", p.id, "addr", p.addr)

			b.events.Emit(Event{
				Type: EventPeerConnected,
				Peer: p,
			})

		case p := <-b.unregister:
			b.LeaveRoom(p)
			b.handler.HandleDisconnect(p)

			b.peersMu.Lock()
			delete(b.peers, p.id)
			b.peersMu.Unlock()

			p.shutdown()

			b.logger.Debug("peer disconnected", "peer_id", p.id)

			b.events.Emit(Event{
				Type: EventPeerDisconnected,
				Peer: p,
			})

		case r := <-b.receive:
			b.events.Emit(Event{
				Type: EventMessageReceived,
				Peer: r.from,
				Msg:  r.msg,
			})

			b.handler.HandleMessage(r.from, r.msg)
		}
	}
}

// cleanup 清理中间人
func (b *Broker) cleanup() {
	b.peersMu.Lock()
	defer b.peersMu.Unlock()
	for _, p := range b.peers {
		p.transport.Close()
		p.shutdown()
	}
	b.peers = make(map[string]*peer)
}

// defaultPeerIDGenerator 默认的连接 ID 生成器
func defaultPeerIDGenerator() string {
	buf := make([]byte, 12)
	rand.Read(buf)
	return fmt.Sprintf("P%s", hex.EncodeToString(buf))
}

// generateRoomID 生成一个房间 ID
func generateRoomID() string {
	buf := make([]byte, 12)
	rand.Read(buf)
	return fmt.Sprintf("R%s", hex.EncodeToString(buf))
}
