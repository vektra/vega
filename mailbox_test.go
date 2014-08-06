package mailbox

import (
	"bytes"
	"testing"
)

func TestMailboxPush(t *testing.T) {
	m := NewMemMailbox()

	msg := []byte("hello")

	m.Push(msg)

	out, _ := m.Poll()
	if !bytes.Equal(out, msg) {
		t.Fatal("Wrong value")
	}
}

func TestMailboxWatcher(t *testing.T) {
	m := NewMemMailbox()

	watch := m.AddWatcher()

	msg := []byte("hello")

	m.Push(msg)

	select {
	case ret := <-watch:
		if !bytes.Equal(ret, msg) {
			t.Fatal("wrong message")
		}
	default:
		t.Fatal("watch didn't get value")
	}
}
