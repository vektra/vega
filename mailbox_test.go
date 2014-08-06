package mailbox

import "testing"

func TestMailboxPush(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	if !out.Equal(msg) {
		t.Fatal("Wrong value")
	}
}

func TestMailboxWatcher(t *testing.T) {
	m := NewMemMailbox("")

	watch := m.AddWatcher()

	msg := Msg([]byte("hello"))

	m.Push(msg)

	select {
	case ret := <-watch:
		if !ret.Equal(msg) {
			t.Fatal("wrong message")
		}
	default:
		t.Fatal("watch didn't get value")
	}
}
