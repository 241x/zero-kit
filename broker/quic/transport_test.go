package quic

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	goquic "github.com/quic-go/quic-go"
)

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	tlsCert, _ := tls.X509KeyPair(certPEM, keyPEM)

	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"broker-test"},
	}
}

func TestQUICTransportReadWrite(t *testing.T) {
	tlsConfig := generateTLSConfig()

	ln, err := goquic.ListenAddr("127.0.0.1:0", tlsConfig, nil)
	if err != nil {
		t.Fatalf("listen error: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			errCh <- err
			return
		}

		transport := NewTransport(stream, conn.RemoteAddr().String())
		data, err := transport.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}

		if err := transport.WriteMessage(append([]byte("quic-echo: "), data...)); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	tlsClientConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"broker-test"},
	}

	conn, err := goquic.DialAddr(context.Background(), addr, tlsClientConfig, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.CloseWithError(0, "")

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream error: %v", err)
	}

	transport := NewTransport(stream, conn.RemoteAddr().String())

	if err := transport.WriteMessage([]byte("ping")); err != nil {
		t.Fatalf("write error: %v", err)
	}

	data, err := transport.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(data) != "quic-echo: ping" {
		t.Fatalf("expected 'quic-echo: ping', got '%s'", string(data))
	}

	if err := <-errCh; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func TestQUICTransportAddr(t *testing.T) {
	tlsConfig := generateTLSConfig()

	ln, err := goquic.ListenAddr("127.0.0.1:0", tlsConfig, nil)
	if err != nil {
		t.Fatalf("listen error: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	done := make(chan string, 1)
	go func() {
		conn, _ := ln.Accept(context.Background())
		stream, _ := conn.AcceptStream(context.Background())
		transport := NewTransport(stream, conn.RemoteAddr().String())
		done <- transport.RemoteAddr()
	}()

	tlsClientConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"broker-test"},
	}

	conn, err := goquic.DialAddr(context.Background(), addr, tlsClientConfig, nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.CloseWithError(0, "")

	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("open stream error: %v", err)
	}
	stream.Write([]byte{0, 0, 0, 0})

	select {
	case remoteAddr := <-done:
		if remoteAddr == "" {
			t.Fatal("expected non-empty remote addr")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for transport addr")
	}
}
