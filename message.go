package mailbox

import (
	"bytes"
	"time"

	"github.com/ugorji/go/codec"
)

// A message that is transmitted. This mostly adopts the AMQP
// basic properties mostly beacuse they're common values
// that are used to implement patterns on top of the system.

type Message struct {
	// Simple generic headers available to be used by the application
	Headers map[string]interface{} `codec:"headers,omitempty`

	// Properties
	ContentType     string    `codec:"content_type,omitempty"`     // MIME content type
	ContentEncoding string    `codec:"content_encoding,omitempty"` // MIME content encoding
	Priority        uint8     `codec:"priority,omitempty"`         // 0 to 9
	CorrelationId   string    `codec:"correlation_id,omitempty"`   // correlation identifier
	ReplyTo         string    `codec:"reply_to,omitempty"`         // address to to reply to
	MessageId       string    `codec:"message_id,omitempty"`       // message identifier
	Timestamp       time.Time `codec:"timestamp,omitempty"`        // message timestamp
	Type            string    `codec:"type,omitempty"`             // message type name
	UserId          string    `codec:"user_id,omitempty"`          // creating user id
	AppId           string    `codec:"app_id,omitempty"`           // creating application id

	Body []byte `codec:body,omitempty`
}

func (m *Message) AddHeader(name string, val interface{}) {
	if m.Headers == nil {
		m.Headers = make(map[string]interface{})
	}

	m.Headers[name] = val
}

func (m *Message) GetHeader(name string) (interface{}, bool) {
	v, ok := m.Headers[name]
	return v, ok
}

func Msg(body []byte) *Message {
	return &Message{Body: body}
}

func (m *Message) Equal(m2 *Message) bool {
	return bytes.Equal(m.Body, m2.Body)
}

func (m *Message) AsBytes() (ret []byte) {
	enc := codec.NewEncoderBytes(&ret, &msgpack)

	err := enc.Encode(m)
	if err != nil {
		panic(err)
	}

	return
}

func (m *Message) FromBytes(b []byte) error {
	dec := codec.NewDecoderBytes(b, &msgpack)

	return dec.Decode(m)
}

func DecodeMessage(b []byte) *Message {
	m := &Message{}

	err := m.FromBytes(b)
	if err != nil {
		panic(err)
	}

	return m
}
