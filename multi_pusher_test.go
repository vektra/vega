package mailbox

import "testing"

func TestMultiPusherPush(t *testing.T) {
	m1 := NewMemRegistry()
	m2 := NewMemRegistry()

	m1.Declare("a")
	m2.Declare("a")

	mp := NewMultiPusher()
	mp.Add(m1)
	mp.Add(m2)

	msg := Msg([]byte("hello"))

	mp.Push("a", msg)

	a1, _ := m1.Poll("a")
	a2, _ := m2.Poll("a")

	if a1 == nil || a2 == nil || !msg.Equal(a1) || !msg.Equal(a2) {
		t.Fatal("message did not fan out among endpoints")
	}
}
