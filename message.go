package mailbox

import (
	"bytes"

	"github.com/ugorji/go/codec"
)

type Message struct {
	Body []byte
}

func Msg(body []byte) *Message {
	return &Message{body}
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
