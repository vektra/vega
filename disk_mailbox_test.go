package mailbox

import (
	"io/ioutil"
	"os"
	"testing"
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

	_, ok := m.Poll()
	if ok {
		t.Fatal("there shouldn't be anything in queue")
	}

	msg := Msg([]byte("hello"))

	m.Push(msg)

	m.Poll()

	_, ok = m.Poll()
	if ok {
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

	_, ok = m.Poll()
	if ok {
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
