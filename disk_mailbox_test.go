package vega

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/jmhodges/levigo"
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

	msg := Msg([]byte("hello"))

	m.Push(msg)

	out, _ := m.Poll()
	if !out.Equal(msg) {
		t.Fatal("Wrong value")
	}
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

	err = m.Ack(out.MessageId)
	if err != nil {
		panic(err)
	}

	stats = m.Stats()

	if stats.InFlight != 0 {
		t.Fatal("ack did not remove the inflight message")
	}
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

	err = m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	if out3 == nil || !out.Equal(out3) {
		t.Fatal("Nack'd message not come back")
	}
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

	msg := Msg("hello")
	msg2 := Msg("2nd message")

	m.Push(msg)
	m.Push(msg2)

	out, _ := m.Poll()
	if !out.Equal(msg) {
		t.Fatal("Wrong value")
	}

	err = m.Nack(out.MessageId)
	if err != nil {
		panic(err)
	}

	out3, _ := m.Poll()
	if !out.Equal(out3) {
		t.Fatal("Nack'd message not come back")
	}
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

	msg := Msg("hello")
	msg2 := Msg("2nd message")

	m.Push(msg)
	m.Push(msg2)

	out1, _ := m.Poll()
	if !out1.Equal(msg) {
		t.Fatal("Wrong value")
	}

	out2, _ := m.Poll()
	if !out2.Equal(msg2) {
		t.Fatal("wrong message")
	}

	err = m.Nack(out1.MessageId)
	if err != nil {
		panic(err)
	}

	stats := m.Stats()
	if stats.Size != 1 {
		t.Fatalf("nack didn't change size")
	}

	out3, _ := m.Poll()
	if !out1.Equal(out3) {
		t.Fatal("Nack'd message not come back")
	}

	stats = m.Stats()
	if stats.InFlight != 2 {
		t.Fatalf("failed to track discontinious message as inflight %d", stats.InFlight)
	}

	err = m.Ack(out3.MessageId)
	if err != nil {
		panic(err)
	}

	stats = m.Stats()
	if stats.InFlight != 1 {
		t.Fatalf("ack'd discontinious message failed: %d", stats.InFlight)
	}

	err = m.Ack(out2.MessageId)
	if err != nil {
		panic(err)
	}

	stats = m.Stats()
	if stats.InFlight != 0 {
		t.Fatalf("ack'd discontinious message failed: %d", stats.InFlight)
	}
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

	if v != nil {
		t.Fatal("there shouldn't be anything in queue")
	}

	msg := Msg([]byte("hello"))

	m.Push(msg)

	m.Poll()

	v, err = m.Poll()
	if err != nil {
		panic(err)
	}

	if v != nil {
		t.Fatal("there shouldn't be anything in the queue")
	}

	msg2 := Msg([]byte("message 2"))
	msg3 := Msg([]byte("third message"))

	m.Push(msg2)
	m.Push(msg3)

	ret, _ := m.Poll()
	if !ret.Equal(msg2) {
		t.Fatal("Unable to pull correct message")
	}

	ret, _ = m.Poll()
	if !ret.Equal(msg3) {
		t.Fatal("Unable to pull correct message")
	}

	ret, _ = m.Poll()
	if ret != nil {
		t.Fatal("there shouldn't be anything in the queue")
	}
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
		t.Fatal("there shouldn't be anything in queue")
	}

	msg := Msg([]byte("hello"))

	m.Push(msg)

	if m.Stats().Size != 1 {
		t.Fatal("stats not updated to 1")
	}

	m.Push(msg)

	if m.Stats().Size != 2 {
		t.Fatal("stats not updated to 2")
	}

	m.Poll()
	m.Poll()

	if m.Stats().Size != 0 {
		t.Fatal("stats not updated to 0")
	}
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

	msg := Msg([]byte("hello"))

	m.Push(msg)

	r.Close()

	r2, err := NewDiskStorage(dir)
	if err != nil {
		panic(err)
	}

	defer r2.Close()

	m2 := r2.Mailbox("a")

	ret, _ := m2.Poll()

	if !ret.Equal(msg) {
		t.Fatal("couldn't pull the message out")
	}
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

	err = m.Push(Msg([]byte("hello")))
	if err != nil {
		panic(err)
	}

	ro := levigo.NewReadOptions()

	data, err := r.db.Get(ro, []byte("a0"))
	if err != nil {
		panic(err)
	}

	if len(data) == 0 {
		t.Fatal("mailbox was not setup")
	}

	m.Abandon()

	data, err = r.db.Get(ro, []byte("a0"))
	if err != nil {
		panic(err)
	}

	if len(data) != 0 {
		t.Fatal("mailbox was not deleted")
	}
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
		if ret != nil {
			t.Fatal("wrong message")
		}
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

	msg := Msg([]byte("hello"))

	m.Push(msg)

	select {
	case ret := <-watch:
		if ret != nil {
			t.Fatal("done didn't close indicator")
		}
	default:
		t.Fatal("watch didn't get value")
	}

	out, err := m.Poll()
	if err != nil {
		panic(err)
	}

	if out == nil || !out.Equal(msg) {
		t.Fatal("didn't get the right message")
	}
}
