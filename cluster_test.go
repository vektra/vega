package mailbox

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

func TestClusterLocalMessages(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn, err := NewMemClusterNode(dir)
	if err != nil {
		panic(err)
	}

	defer cn.Close()

	err = cn.Declare("a")
	if err != nil {
		panic(err)
	}

	payload := Msg([]byte("hello"))

	err = cn.Push("a", payload)
	if err != nil {
		panic(err)
	}

	msg, err := cn.disk.Mailbox("a").Poll()

	if msg == nil || !msg.Equal(payload) {
		t.Fatal("message was not stored locally")
	}
}

func TestClusterRoutes(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn, err := NewMemClusterNode(dir)
	if err != nil {
		panic(err)
	}

	defer cn.Close()

	memReg := NewMemRegistry()
	memReg.Declare("a")

	cn.AddRoute("a", memReg)

	payload := Msg([]byte("hello"))

	cn.Push("a", payload)

	msg, err := memReg.Poll("a")

	if msg == nil || !msg.Equal(payload) {
		t.Fatal("message was not stored locally")
	}
}

func TestClusterLongPoll(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn, err := NewMemClusterNode(dir)
	if err != nil {
		panic(err)
	}

	defer cn.Close()

	msg := Msg([]byte("hello"))

	var got *Message

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		got, _ = cn.LongPoll("a", 2*time.Second)
	}()

	cn.Declare("a")
	cn.Push("a", msg)

	wg.Wait()

	if got == nil || !msg.Equal(got) {
		t.Fatal("long poll didn't see the value")
	}
}

func TestClusterRoutesViaNetwork(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn, err := NewMemClusterNode(dir)
	if err != nil {
		panic(err)
	}

	defer cn.Close()

	dir2, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir2)

	cn2, err := NewMemClusterNode(dir2)
	if err != nil {
		panic(err)
	}

	defer cn2.Close()

	// Setup 2 service objects

	s1, err := NewService(cPort, cn)
	if err != nil {
		panic(err)
	}

	defer s1.Close()
	go s1.Accept()

	s2, err := NewService(cPort2, cn2)
	if err != nil {
		panic(err)
	}

	defer s2.Close()
	go s2.Accept()

	// Wire up a client going to s1

	toS1, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	toS1.Declare("a")
	cn2.AddRoute("a", toS1)

	// Push data into cn2 and see it show up in cn

	toS2, err := NewClient(cPort2)
	if err != nil {
		panic(err)
	}

	msg := Msg([]byte("between nodes"))

	err = toS2.Push("a", msg)
	if err != nil {
		panic(err)
	}

	debugf("polling\n")

	ret, err := toS1.Poll("a")
	if err != nil {
		panic(err)
	}

	if ret == nil || !ret.Equal(msg) {
		t.Fatal("message did not route properly")
	}
}

func TestClusterAbandon(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn, err := NewMemClusterNode(dir)
	if err != nil {
		panic(err)
	}

	defer cn.Close()

	err = cn.Declare("a")
	if err != nil {
		panic(err)
	}

	payload := Msg([]byte("hello"))

	err = cn.Push("a", payload)
	if err != nil {
		panic(err)
	}

	err = cn.Abandon("a")
	if err != nil {
		panic(err)
	}

	err = cn.Push("a", payload)
	if err == nil {
		t.Fatal("queue was not abondoned")
	}
}
