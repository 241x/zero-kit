package broker

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockTransport struct {
	readCh  chan []byte
	writeCh chan []byte
	addr    string
	closed  bool
	mu      sync.Mutex
}

func newMockTransport(addr string) *mockTransport {
	return &mockTransport{
		readCh:  make(chan []byte, 10),
		writeCh: make(chan []byte, 10),
		addr:    addr,
	}
}

func (m *mockTransport) ReadMessage() ([]byte, error) {
	msg, ok := <-m.readCh
	if !ok {
		return nil, context.Canceled
	}
	return msg, nil
}

func (m *mockTransport) WriteMessage(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return context.Canceled
	}
	m.writeCh <- data
	return nil
}

func (m *mockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	close(m.readCh)
	return nil
}

func (m *mockTransport) RemoteAddr() string {
	return m.addr
}

type mockHandler struct {
	messages    []*Message
	disconnects int
	mu          sync.Mutex
}

func (h *mockHandler) HandleMessage(from Peer, msg *Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, msg)
}

func (h *mockHandler) HandleDisconnect(from Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnects++
}

func TestNewBroker(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	if b == nil {
		t.Fatal("expected non-nil broker")
	}
	if b.started {
		t.Fatal("broker should not be started")
	}
}

func TestBrokerStartStop(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)

	if !b.started {
		t.Fatal("broker should be started")
	}

	if err := b.Stop(); err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}

	if b.started {
		t.Fatal("broker should be stopped")
	}
}

func TestBrokerAccept(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	defer b.Stop()

	transport := newMockTransport("127.0.0.1:9001")
	peer, err := b.Accept(transport)
	if err != nil {
		t.Fatalf("unexpected accept error: %v", err)
	}
	if peer.ID() == "" {
		t.Fatal("peer should have an ID")
	}
	if peer.RemoteAddr() != "127.0.0.1:9001" {
		t.Fatalf("expected addr 127.0.0.1:9001, got %s", peer.RemoteAddr())
	}

	time.Sleep(50 * time.Millisecond)

	if p := b.Peer(peer.ID()); p == nil {
		t.Fatal("peer should be registered")
	}
}

func TestBrokerSendTo(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	defer b.Stop()

	transport := newMockTransport("127.0.0.1:9002")
	peer, err := b.Accept(transport)
	if err != nil {
		t.Fatalf("unexpected accept error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	msg := NewMessage("test")
	msg.Payload["key"] = "value"
	if err := b.SendTo(peer.ID(), msg); err != nil {
		t.Fatalf("unexpected send error: %v", err)
	}

	select {
	case data := <-transport.writeCh:
		decoded, err := UnmarshalMessage(data)
		if err != nil {
			t.Fatalf("unexpected unmarshal error: %v", err)
		}
		if decoded.Type != "test" {
			t.Fatalf("expected type 'test', got '%s'", decoded.Type)
		}
		if decoded.Payload["key"] != "value" {
			t.Fatalf("expected payload key 'value', got '%v'", decoded.Payload["key"])
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestBrokerMessageReceive(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	defer b.Stop()

	transport := newMockTransport("127.0.0.1:9003")
	_, err := b.Accept(transport)
	if err != nil {
		t.Fatalf("unexpected accept error: %v", err)
	}

	msg := NewMessage("chat")
	msg.Payload["text"] = "hello"
	transport.readCh <- msg.Marshal()

	time.Sleep(50 * time.Millisecond)

	handler.mu.Lock()
	defer handler.mu.Unlock()
	if len(handler.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(handler.messages))
	}
	if handler.messages[0].Type != "chat" {
		t.Fatalf("expected type 'chat', got '%s'", handler.messages[0].Type)
	}
}

func TestBrokerDisconnect(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	defer b.Stop()

	transport := newMockTransport("127.0.0.1:9004")
	peer, err := b.Accept(transport)
	if err != nil {
		t.Fatalf("unexpected accept error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	disconnected := make(chan struct{})
	b.Events().On(EventPeerDisconnected, func(e Event) {
		close(disconnected)
	})

	peer.Close()

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for disconnect")
	}

	if p := b.Peer(peer.ID()); p != nil {
		t.Fatal("peer should be unregistered after disconnect")
	}

	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.disconnects != 1 {
		t.Fatalf("expected 1 disconnect, got %d", handler.disconnects)
	}
}

func TestBrokerRoomLifecycle(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	defer b.Stop()

	room := b.CreateRoom()
	if room == nil || room.ID() == "" {
		t.Fatal("room should have an ID")
	}

	t1 := newMockTransport("127.0.0.1:9005")
	p1, _ := b.Accept(t1)

	t2 := newMockTransport("127.0.0.1:9006")
	p2, _ := b.Accept(t2)

	time.Sleep(50 * time.Millisecond)

	if err := b.JoinRoom(room.ID(), p1); err != nil {
		t.Fatalf("unexpected join error: %v", err)
	}
	if err := b.JoinRoom(room.ID(), p2); err != nil {
		t.Fatalf("unexpected join error: %v", err)
	}

	if p1.RoomID() != room.ID() {
		t.Fatalf("expected room id %s, got %s", room.ID(), p1.RoomID())
	}
	if room.Size() != 2 {
		t.Fatalf("expected room size 2, got %d", room.Size())
	}

	msg := NewMessage("broadcast")
	b.SendToRoom(room.ID(), msg, p1)

	select {
	case data := <-t2.writeCh:
		decoded, _ := UnmarshalMessage(data)
		if decoded.Type != "broadcast" {
			t.Fatalf("expected type 'broadcast', got '%s'", decoded.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for room broadcast to p2")
	}

	b.LeaveRoom(p1)

	if p1.RoomID() != "" {
		t.Fatal("peer should have empty room id after leave")
	}
	if room.Size() != 1 {
		t.Fatalf("expected room size 1, got %d", room.Size())
	}
}

func TestBrokerAcceptAfterStop(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	b.Stop()

	transport := newMockTransport("127.0.0.1:9007")
	_, err := b.Accept(transport)
	if err != ErrBrokerClosed {
		t.Fatalf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestBrokerEvents(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var connectCount, disconnectCount int
	connected := make(chan struct{})
	disconnected := make(chan struct{})

	b.Events().On(EventPeerConnected, func(e Event) {
		connectCount++
		close(connected)
	})
	b.Events().On(EventPeerDisconnected, func(e Event) {
		disconnectCount++
		close(disconnected)
	})

	b.Start(ctx)
	defer b.Stop()

	transport := newMockTransport("127.0.0.1:9008")
	peer, _ := b.Accept(transport)

	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for connect event")
	}

	peer.Close()

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for disconnect event")
	}

	if connectCount != 1 {
		t.Fatalf("expected 1 connect event, got %d", connectCount)
	}
	if disconnectCount != 1 {
		t.Fatalf("expected 1 disconnect event, got %d", disconnectCount)
	}
}

func TestBrokerOptions(t *testing.T) {
	idCh := make(chan string, 1)
	handler := &mockHandler{}
	b := New(handler,
		WithChannelBufSize(128),
		WithMaxMessageSize(1024),
		WithPeerIDGenerator(func() string {
			id := "CUSTOM-001"
			idCh <- id
			return id
		}),
		WithRoomAutoCleanup(false),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	defer b.Stop()

	transport := newMockTransport("127.0.0.1:9009")
	peer, err := b.Accept(transport)
	if err != nil {
		t.Fatalf("unexpected accept error: %v", err)
	}

	select {
	case id := <-idCh:
		if id != peer.ID() {
			t.Fatalf("expected ID %s, got %s", id, peer.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for custom ID")
	}

	room := b.CreateRoom()
	b.JoinRoom(room.ID(), peer)
	b.LeaveRoom(peer)

	if b.Room(room.ID()) == nil {
		t.Fatal("room should not be auto-cleaned when RoomAutoCleanup is false")
	}
}

func TestBrokerMaxMessageSize(t *testing.T) {
	handler := &mockHandler{}
	b := New(handler, WithMaxMessageSize(10))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Start(ctx)
	defer b.Stop()

	transport := newMockTransport("127.0.0.1:9010")
	_, err := b.Accept(transport)
	if err != nil {
		t.Fatalf("unexpected accept error: %v", err)
	}

	largeMsg := make([]byte, 100)
	transport.readCh <- largeMsg

	time.Sleep(100 * time.Millisecond)

	handler.mu.Lock()
	count := len(handler.messages)
	handler.mu.Unlock()
	if count > 0 {
		t.Fatalf("large message should be rejected, got %d messages", count)
	}
}
