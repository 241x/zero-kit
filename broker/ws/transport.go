package ws

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultWriteWait  = 10 * time.Second
	defaultPingPeriod = 54 * time.Second
	defaultPongWait   = 60 * time.Second
)

// Upgrader 升级 HTTP 连接为 WebSocket 连接
var Upgrader = websocket.Upgrader{
	ReadBufferSize:  10240,
	WriteBufferSize: 10240,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Config 配置
type Config struct {
	WriteWait  time.Duration
	PingPeriod time.Duration
	PongWait   time.Duration
}

// defaultConfig 默认配置
func defaultConfig() Config {
	return Config{
		WriteWait:  defaultWriteWait,
		PingPeriod: defaultPingPeriod,
		PongWait:   defaultPongWait,
	}
}

// Transport WebSocket 传输
type Transport struct {
	conn       *websocket.Conn
	addr       string
	pingPeriod time.Duration
	writeWait  time.Duration
	pongWait   time.Duration
	closeCh    chan struct{}
}

// New 创建一个新的 WebSocket 传输
func New(conn *websocket.Conn, cfgs ...Config) *Transport {
	cfg := defaultConfig()
	if len(cfgs) > 0 {
		if cfgs[0].WriteWait > 0 {
			cfg.WriteWait = cfgs[0].WriteWait
		}
		if cfgs[0].PingPeriod > 0 {
			cfg.PingPeriod = cfgs[0].PingPeriod
		}
		if cfgs[0].PongWait > 0 {
			cfg.PongWait = cfgs[0].PongWait
		}
	}

	conn.SetReadLimit(10240)
	conn.SetReadDeadline(time.Now().Add(cfg.PongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(cfg.PongWait))
		return nil
	})

	t := &Transport{
		conn:       conn,
		addr:       conn.RemoteAddr().String(),
		writeWait:  cfg.WriteWait,
		pingPeriod: cfg.PingPeriod,
		pongWait:   cfg.PongWait,
		closeCh:    make(chan struct{}),
	}

	go t.pingLoop()

	return t
}

// ReadMessage 读取消息
func (t *Transport) ReadMessage() ([]byte, error) {
	t.conn.SetReadDeadline(time.Now().Add(t.pongWait))
	_, data, err := t.conn.ReadMessage()
	return data, err
}

// WriteMessage 写入消息
func (t *Transport) WriteMessage(data []byte) error {
	t.conn.SetWriteDeadline(time.Now().Add(t.writeWait))
	return t.conn.WriteMessage(websocket.TextMessage, data)
}

// Close 关闭连接
func (t *Transport) Close() error {
	select {
	case <-t.closeCh:
	default:
		close(t.closeCh)
	}
	return t.conn.Close()
}

// RemoteAddr 获取远程地址
func (t *Transport) RemoteAddr() string {
	return t.addr
}

// pingLoop 发送 ping 消息
func (t *Transport) pingLoop() {
	ticker := time.NewTicker(t.pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-t.closeCh:
			return
		case <-ticker.C:
			t.conn.SetWriteDeadline(time.Now().Add(t.writeWait))
			if err := t.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
