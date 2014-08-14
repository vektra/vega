package vega

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMailboxPush(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()

	assert.True(t, msg.Equal(out))
}

func TestMailboxAck(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()

	assert.True(t, msg.Equal(out))

	stats := m.Stats()

	assert.Equal(t, 0, stats.Size)
	assert.Equal(t, 1, stats.InFlight)

	err := m.Ack(out.MessageId)
	if err != nil {
		panic(err)
	}

	stats = m.Stats()

	assert.Equal(t, 0, stats.InFlight)
}

func TestMailboxNack(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	assert.True(t, msg.Equal(out))

	out2, _ := m.Poll()
	assert.Nil(t, out2)

	err := m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	assert.True(t, out.Equal(out3))
}

func TestMailboxNackPutsAMessageAtTheFront(t *testing.T) {
	m := NewMemMailbox("")

	msg := Msg("hello")
	msg2 := Msg("2nd message")

	m.Push(msg)
	m.Push(msg2)

	out, _ := m.Poll()
	assert.True(t, msg.Equal(out))

	err := m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	assert.True(t, out.Equal(out3))
}

func TestMailboxWatcher(t *testing.T) {
	m := NewMemMailbox("")

	watch := m.AddWatcher()

	msg := Msg([]byte("hello"))

	m.Push(msg)

	select {
	case ret := <-watch:
		assert.True(t, msg.Equal(ret))
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
		assert.Nil(t, ret)
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

	assert.Equal(t, 1, stats.InFlight)

	select {
	case ret := <-watch:
		err := m.Ack(ret.MessageId)
		assert.NoError(t, err)
	default:
		t.Fatal("watch didn't get value")
	}

	stats = m.Stats()

	assert.Equal(t, 0, stats.InFlight)
}

func TestMailboxWatcherIsCancelable(t *testing.T) {
	m := NewMemMailbox("")

	done := make(chan struct{})

	watch := m.AddWatcherCancelable(done)

	close(done)

	msg := Msg([]byte("hello"))

	m.Push(msg)

	select {
	case ret := <-watch:
		assert.Nil(t, ret)
	default:
		t.Fatal("watch didn't get value")
	}

	out, err := m.Poll()
	if err != nil {
		panic(err)
	}

	assert.True(t, msg.Equal(out))
}
