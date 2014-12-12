package disk

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/jmhodges/levigo"
	"github.com/stretchr/testify/assert"
	"github.com/vektra/vega"
)

func TestDiskMailboxPush(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	assert.True(t, out.Equal(msg), "wrong value")
}

func TestDiskMailboxAck(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	assert.True(t, out.Equal(msg), "wrong value")

	stats := m.Stats()

	assert.Equal(t, stats.Size, 0, "poll did not consume the message")
	assert.Equal(t, stats.InFlight, 1, "poll did not put the message inflight")

	err = m.Ack(out.MessageId)
	if err != nil {
		panic(err)
	}

	stats = m.Stats()

	assert.Equal(t, stats.InFlight, 0, "ack did not remove the inflight message")
}

func TestDiskMailboxNack(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	assert.True(t, out.Equal(msg), "wrong value")

	out2, _ := m.Poll()
	assert.Nil(t, out2, "where did this message come from?")

	err = m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	assert.True(t, out.Equal(out3), "nack'd message did not come back")
}

func TestDiskMailboxNackPutsAMessageAtTheFront(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	msg := vega.Msg("hello")
	msg2 := vega.Msg("2nd message")

	m.Push(msg)
	m.Push(msg2)

	out, _ := m.Poll()
	assert.True(t, msg.Equal(out), "wrong value")

	err = m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	assert.True(t, out.Equal(out3), "nack'd message did not come back")
}

func TestDiskMailboxDiscontiniousNack(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	msg := vega.Msg("hello")
	msg2 := vega.Msg("2nd message")

	m.Push(msg)
	m.Push(msg2)

	out1, _ := m.Poll()
	assert.True(t, msg.Equal(out1), "wrong value")

	out2, _ := m.Poll()
	assert.True(t, msg2.Equal(out2), "wrong value")

	err = m.Nack(out1.MessageId)
	if err != nil {
		panic(err)
	}

	stats := m.Stats()
	assert.Equal(t, 1, stats.Size, "nack didn't change size")

	out3, _ := m.Poll()
	assert.True(t, out1.Equal(out3), "nack'd message did not come back")

	stats = m.Stats()
	assert.Equal(t, 2, stats.InFlight, "failed to track discontinious message")

	err = m.Ack(out3.MessageId)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 1, m.Stats().InFlight, "ack'd discontinous message failed")

	err = m.Ack(out2.MessageId)
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 0, m.Stats().InFlight, "ack'd discontinous message failed")
}

func TestDiskMailboxWatcherGoesInflight(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	watch := m.AddWatcher()

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	assert.Equal(t, 1, m.Stats().InFlight, "push did not set the message inflight")

	select {
	case ret := <-watch:
		err := m.Ack(ret.MessageId)
		assert.NoError(t, err, "wrong message")
	default:
		t.Fatal("watch didn't get value")
	}
}
func TestDiskMailboxKeepsStatus(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	v, err := m.Poll()
	if err != nil {
		panic(err)
	}

	assert.Nil(t, v, "there shouldn't be anything in mailbox")

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	m.Poll()

	v, err = m.Poll()
	if err != nil {
		panic(err)
	}

	if v != nil {
		t.Fatal("there shouldn't be anything in the mailbox")
	}

	msg2 := vega.Msg([]byte("message 2"))
	msg3 := vega.Msg([]byte("third message"))

	m.Push(msg2)
	m.Push(msg3)

	ret, _ := m.Poll()
	assert.True(t, msg2.Equal(ret), "unable to pull correct message")

	ret, _ = m.Poll()
	assert.True(t, msg3.Equal(ret), "unable to pull correct message")

	ret, _ = m.Poll()
	assert.Nil(t, ret, "mailbox should be empty")
}

func TestDiskMailboxStats(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	if m.Stats().Size != 0 {
		t.Fatal("there shouldn't be anything in mailbox")
	}

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	assert.Equal(t, 1, m.Stats().Size, "stats not updated to 1")

	m.Push(msg)

	assert.Equal(t, 2, m.Stats().Size, "stats not updated to 2")

	m.Poll()
	m.Poll()

	assert.Equal(t, 0, m.Stats().Size, "stats not updated to 0")
}

func TestDiskMailboxWatcher(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r.Close()

	m := r.Mailbox("a")

	watch := m.AddWatcher()

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	select {
	case ret := <-watch:
		assert.True(t, msg.Equal(ret), "wrong message")
	default:
		t.Fatal("watch didn't get value")
	}
}

func TestDiskMailboxPersists(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	m := r.Mailbox("a")

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	r.Close()

	r2, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r2.Close()

	m2 := r2.Mailbox("a")

	ret, _ := m2.Poll()
	assert.True(t, msg.Equal(ret), "couldn't pull the message out")
}

func TestDiskMailboxAbandon(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	m := r.Mailbox("a")

	err = m.Push(vega.Msg([]byte("hello")))
	if err != nil {
		panic(err)
	}

	ro := levigo.NewReadOptions()

	data, err := r.db.Get(ro, []byte("a0"))
	if err != nil {
		panic(err)
	}

	assert.NotEqual(t, 0, len(data), "mailbox not setup")

	m.Abandon()

	data, err = r.db.Get(ro, []byte("a0"))
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 0, len(data), "mailbox not deleted")
}

func TestDiskMailboxUpdatesInfoOnAbandon(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	m := r.Mailbox("a")

	ro := levigo.NewReadOptions()

	data, err := r.db.Get(ro, []byte(":info:"))
	if err != nil {
		panic(err)
	}

	var header infoHeader

	err = diskDataUnmarshal(data, &header)
	if err != nil {
		panic(err)
	}

	_, exists := header.Mailboxes["a"]

	assert.Equal(t, true, exists, "mailbox not setup")

	m.Abandon()

	data, err = r.db.Get(ro, []byte(":info:"))
	if err != nil {
		panic(err)
	}

	header = infoHeader{}

	err = diskDataUnmarshal(data, &header)
	if err != nil {
		panic(err)
	}

	_, exists = header.Mailboxes["a"]

	assert.Equal(t, false, exists, "mailbox not deleted")
}

func TestDiskMailboxInformsWatchersOnAbandon(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	m := r.Mailbox("a")

	watch := m.AddWatcher()

	m.Abandon()

	select {
	case ret := <-watch:
		assert.Nil(t, ret, "wrong message")
	default:
		t.Fatal("watch didn't get value")
	}
}

func TestDiskMailboxWatcherIsCancelable(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	r, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	m := r.Mailbox("a")

	done := make(chan struct{})

	watch := m.AddWatcherCancelable(done)

	close(done)

	msg := vega.Msg([]byte("hello"))

	m.Push(msg)

	select {
	case ret := <-watch:
		assert.Nil(t, ret, "done didn't close indicator")
	default:
		t.Fatal("watch didn't get value")
	}

	out, err := m.Poll()
	if err != nil {
		panic(err)
	}

	assert.True(t, msg.Equal(out), "didn't get right message")
}
