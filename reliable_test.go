package vega

import (
	"errors"
	"testing"
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

	if rs.BufferedMessages() != 1 {
		t.Fatal("didn't buffer the message")
	}

	ur.fail = false

	rs.Retry()

	if rs.BufferedMessages() != 0 {
		t.Fatal("didn't unbuffer the message")
	}

	if ur.name != "a" {
		t.Fatal("didn't pass the correct name")
	}

	if ur.msg == nil || !ur.msg.Equal(msg) {
		t.Fatal("didn't pass the correct message")
	}
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

	if rs.BufferedMessages() != 0 {
		t.Fatal("didn't unbuffer the message")
	}

	if ur.name != "b" {
		t.Fatal("didn't pass the correct name")
	}

	if ur.msg == nil || !ur.msg.Equal(msg2) {
		t.Fatal("didn't pass the correct message")
	}
}
