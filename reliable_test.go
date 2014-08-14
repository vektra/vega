package vega

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type unreliableStorage struct {
	Storage
	fail bool
	name string
	msg  *Message
}

var eTooSunny = errors.New("it's too sunny outside")

func (ur *unreliableStorage) Push(name string, msg *Message) error {
	if ur.fail {
		return eTooSunny
	}

	ur.name = name
	ur.msg = msg
	return nil
}

func TestReliablePush(t *testing.T) {
	ur := &unreliableStorage{NullStorage, true, "", nil}

	rs := NewReliableStorage(ur)

	msg := Msg([]byte("hello"))

	err := rs.Push("a", msg)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 1, rs.BufferedMessages())

	ur.fail = false

	rs.Retry()

	assert.Equal(t, 0, rs.BufferedMessages())

	assert.NotNil(t, ur)

	assert.Equal(t, "a", ur.name)
	assert.True(t, msg.Equal(ur.msg))
}

func TestReliableRetryOnPush(t *testing.T) {
	ur := &unreliableStorage{NullStorage, true, "", nil}

	rs := NewReliableStorage(ur)

	msg := Msg([]byte("hello"))

	err := rs.Push("a", msg)
	if err != nil {
		panic(err)
	}

	ur.fail = false

	msg2 := Msg([]byte("hello 2"))

	rs.Push("b", msg2)

	assert.Equal(t, 0, rs.BufferedMessages())

	assert.Equal(t, "b", ur.name)
	assert.True(t, msg2.Equal(ur.msg))
}
