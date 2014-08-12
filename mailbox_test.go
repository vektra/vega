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

func TestMailboxAck(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	if !out.Equal(msg) {
		t.Fatal("Wrong value")
	}

	stats := m.Stats()

	if stats.Size != 0 {
		t.Fatal("poll did not consume the message")
	}

	if stats.InFlight != 1 {
		t.Fatal("poll did not put the message inflight")
	}

	err := m.Ack(out.MessageId)
	if err != nil {
		panic(err)
	}

	stats = m.Stats()

	if stats.InFlight != 0 {
		t.Fatal("ack did not remove the inflight message")
	}
}

func TestMailboxNack(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	if !out.Equal(msg) {
		t.Fatal("Wrong value")
	}

	out2, _ := m.Poll()
	if out2 != nil {
		t.Fatal("where did this message come from?")
	}

	err := m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	if !out.Equal(out3) {
		t.Fatal("Nack'd message not come back")
	}
}

func TestMailboxNackPutsAMessageAtTheFront(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg("hello")
	msg2 := Msg("2nd message")

	m.Push(msg)
	m.Push(msg2)

	out, _ := m.Poll()
	if !out.Equal(msg) {
		t.Fatal("Wrong value")
	}

	err := m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	if !out.Equal(out3) {
		t.Fatal("Nack'd message not come back")
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

func TestMailboxInformsWatchersOnAbandon(t *testing.T) {
	m := NewMemMailbox("")

	watch := m.AddWatcher()

	m.Abandon()

	select {
	case ret := <-watch:
		if ret != nil {
			t.Fatal("wrong message")
		}
	default:
		t.Fatal("watch didn't get value")
	}
}

func TestMailboxWatcherGoesInflight(t *testing.T) {
	m := NewMemMailbox("")

	watch := m.AddWatcher()

	msg := Msg([]byte("hello"))

	m.Push(msg)

	stats := m.Stats()

	if stats.InFlight != 1 {
		t.Fatal("push did not set the message inflight")
	}

	select {
	case ret := <-watch:
		err := m.Ack(ret.MessageId)
		if err != nil {
			t.Fatal("wrong message")
		}
	default:
		t.Fatal("watch didn't get value")
	}

	stats = m.Stats()

	if stats.InFlight != 0 {
		t.Fatal("ack did not work on a watched message")
	}

}
