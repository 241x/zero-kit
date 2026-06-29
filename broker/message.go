package broker

import "encoding/json"

// Message 消息
type Message struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
	To      string         `json:"to,omitempty"`
}

// NewMessage 创建一个新的消息
func NewMessage(typ string) *Message {
	return &Message{
		Type:    typ,
		Payload: make(map[string]any),
	}
}

// Marshal 消息序列化
func (m *Message) Marshal() []byte {
	b, _ := json.Marshal(m)
	return b
}

// UnmarshalMessage 消息反序列化
func UnmarshalMessage(data []byte) (*Message, error) {
	msg := &Message{}
	err := json.Unmarshal(data, msg)
	return msg, err
}
