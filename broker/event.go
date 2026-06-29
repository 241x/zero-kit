package broker

import "sync"

type EventType int

const (
	EventPeerConnected EventType = iota
	EventPeerDisconnected
	EventPeerJoinedRoom
	EventPeerLeftRoom
	EventMessageReceived
)

// Event 事件
type Event struct {
	Type EventType
	Peer Peer
	Room *Room
	Msg  *Message
}

// EventHandler 事件处理函数
type EventHandler func(Event)

// eventBus 事件总线
type eventBus struct {
	mu       sync.RWMutex
	handlers map[EventType][]EventHandler
}

// NewEventBus 创建一个新的事件总线
func newEventBus() *eventBus {
	return &eventBus{
		handlers: make(map[EventType][]EventHandler),
	}
}

// On 注册事件处理函数
func (eb *eventBus) On(typ EventType, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.handlers[typ] = append(eb.handlers[typ], handler)
}

// Emit 触发事件
func (eb *eventBus) Emit(e Event) {
	eb.mu.RLock()
	handlers := eb.handlers[e.Type]
	eb.mu.RUnlock()
	for _, h := range handlers {
		h(e)
	}
}
