package broker

import "sync"

// Room 代表一个房间
type Room struct {
	id    string
	peers map[string]Peer
	mu    sync.RWMutex
}

// newRoom 创建一个房间
func newRoom(id string) *Room {
	return &Room{
		id:    id,
		peers: make(map[string]Peer),
	}
}

// ID 获取房间 ID
func (r *Room) ID() string {
	return r.id
}

// Add 添加一个连接
func (r *Room) Add(p Peer) {
	r.mu.Lock()
	r.peers[p.ID()] = p
	r.mu.Unlock()
}

// Remove 移除一个连接
func (r *Room) Remove(p Peer) {
	r.mu.Lock()
	delete(r.peers, p.ID())
	r.mu.Unlock()
}

// Peers 获取房间所有连接
func (r *Room) Peers() []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	peers := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	return peers
}

// Size 获取房间连接数量
func (r *Room) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}

// Broadcast 广播消息给房间内所有连接
func (r *Room) Broadcast(msg *Message) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.peers {
		_ = p.Send(msg)
	}
}
