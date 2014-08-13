package vega

import (
	"bytes"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ugorji/go/codec"
)

// A message that is transmitted. This mostly adopts the AMQP
// basic properties mostly beacuse they're common values
// that are used to implement patterns on top of the system.

type Message struct {
	// Simple generic headers available to be used by the application
	Headers map[string]interface{} `codec:"headers,omitempty" json:"headers,omitempty"`

	// Properties
	ContentType     string     `codec:"content_type,omitempty" json:"content_type,omitempty"`         // MIME content type
	ContentEncoding string     `codec:"content_encoding,omitempty" json:"content_encoding,omitempty"` // MIME content encoding
	Priority        uint8      `codec:"priority,omitempty" json:"priority,omitempty"`                 // 0 to 9
	CorrelationId   string     `codec:"correlation_id,omitempty" json:"correlation_id,omitempty"`     // correlation identifier
	ReplyTo         string     `codec:"reply_to,omitempty" json:"reply_to,omitempty"`                 // address to to reply to
	MessageId       MessageId  `codec:"message_id,omitempty" json:"message_id,omitempty"`             // message identifier
	Timestamp       *time.Time `codec:"timestamp,omitempty" json:"timestamp,omitempty"`               // message timestamp
	Type            string     `codec:"type,omitempty" json:"type,omitempty"`                         // message type name
	UserId          string     `codec:"user_id,omitempty" json:"user_id,omitempty"`                   // creating user id
	AppId           string     `codec:"app_id,omitempty" json:"app_id,omitempty"`                     // creating application id

	Body []byte `codec:"body,omitempty" json:"body,omitempty"`
}

// Add an application header
func (m *Message) AddHeader(name string, val interface{}) {
	if m.Headers == nil {
		m.Headers = make(map[string]interface{})
	}

	m.Headers[name] = val
}

// Retreive an application header
func (m *Message) GetHeader(name string) (interface{}, bool) {
	v, ok := m.Headers[name]
	return v, ok
}

// Create a message with a body
func Msg(body interface{}) *Message {
	var bytes []byte

	switch subject := body.(type) {
	case string:
		bytes = []byte(subject)
	case []byte:
		bytes = subject
	case Byter:
		bytes = subject.Bytes()
	default:
		panic(fmt.Sprintf("No convertion to bytes for %T", subject))
	}

	return &Message{Body: bytes}
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

type messageIDGen struct {
	base string
	idx  *int64
}

func (g *messageIDGen) NextMessageID() MessageId {
	x := atomic.AddInt64(g.idx, 1)

	return MessageId(g.base + strconv.FormatInt(x, 10))
}

var globalMessageIDGen *messageIDGen

func init() {
	i := int64(0)

	globalMessageIDGen = &messageIDGen{"msg-" + generateUUIDSecure(), &i}
}

func NextMessageID() MessageId {
	return globalMessageIDGen.NextMessageID()
}
