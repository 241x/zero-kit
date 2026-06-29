package tcp

import (
	"net"
	"testing"
)

func TestTransportReadWrite(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	transport := NewTransport(server)

	msg := []byte("hello world")
	go func() {
		if err := transport.WriteMessage(msg); err != nil {
			t.Errorf("write error: %v", err)
		}
	}()

	data, err := NewTransport(client).ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("expected 'hello world', got '%s'", string(data))
	}
}

func TestTransportLargeMessage(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	transport := NewTransport(server)

	msg := make([]byte, 1024*1024)
	for i := range msg {
		msg[i] = byte(i % 256)
	}

	go func() {
		if err := transport.WriteMessage(msg); err != nil {
			t.Errorf("write error: %v", err)
		}
	}()

	data, err := NewTransport(client).ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if len(data) != len(msg) {
		t.Fatalf("expected %d bytes, got %d", len(msg), len(data))
	}
}

func TestTransportClose(t *testing.T) {
	server, client := net.Pipe()

	transport := NewTransport(server)
	transport.Close()

	_, err := client.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected read error after close")
	}
}

func TestTransportRemoteAddr(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	transport := NewTransport(server)
	addr := transport.RemoteAddr()

	if addr == "" {
		t.Fatal("expected non-empty remote addr")
	}

	_ = client
}

func TestTransportIntegration(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen error: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		transport := NewTransport(conn)
		data, err := transport.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}

		if err := transport.WriteMessage(append([]byte("echo: "), data...)); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	transport := NewTransport(conn)
	if err := transport.WriteMessage([]byte("ping")); err != nil {
		t.Fatalf("write error: %v", err)
	}

	data, err := transport.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(data) != "echo: ping" {
		t.Fatalf("expected 'echo: ping', got '%s'", string(data))
	}

	if err := <-errCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}
