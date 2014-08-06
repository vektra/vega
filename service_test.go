package mailbox

import (
	"sync"
	"testing"
	"time"
)

const cPort = "127.0.0.1:34000"
const cPort2 = "127.0.0.1:34001"

func TestServicePushAndPoll(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	c1, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	defer c1.Close()

	c2, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	defer c2.Close()

	c1.Declare("a")

	msg, err := c1.Poll("a")
	if msg != nil {
		t.Fatal("found a message")
	}

	payload := Msg([]byte("hello"))

	c2.Push("a", payload)

	msg, err = c2.Poll("a")
	if err != nil {
		panic(err)
	}

	if msg == nil {
		t.Fatal("didn't find a message")
	}

	if !msg.Equal(payload) {
		t.Fatal("body was corrupted")
	}
}

func TestServiceLongPoll(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	c1, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	defer c1.Close()

	c2, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	defer c2.Close()

	c1.Declare("a")

	var got *Message

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		got, _ = c1.LongPoll("a", 2*time.Second)
	}()

	payload := Msg([]byte("hello"))

	c2.Push("a", payload)

	wg.Wait()

	if got == nil || !got.Equal(payload) {
		t.Fatal("body was corrupted")
	}
}
