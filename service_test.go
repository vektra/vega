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

	c1, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c1.Close()

	c2, err := NewClient(cPort)
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

	c1, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c1.Close()

	c2, err := NewClient(cPort)
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

func TestClientReconnects(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	c1, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c1.Close()

	c1.Declare("a")

	c1.Close()

	payload := Msg([]byte("hello"))

	err = c1.Push("a", payload)
	if err != nil {
		t.Fatal("client didn't reconnect")
	}

	got, err := c1.Poll("a")

	if !got.Equal(payload) {
		t.Fatal("message was lost")
	}
}

func TestServiceAbandon(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	c1, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c1.Close()

	c2, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c2.Close()

	c1.Declare("a")

	payload := Msg([]byte("hello"))

	err = c2.Push("a", payload)
	if err != nil {
		panic(err)
	}

	err = c1.Abandon("a")
	if err != nil {
		panic(err)
	}

	err = c2.Push("a", payload)
	if err == nil {
		t.Fatal("queue was not deleted")
	}
}

func TestServiceEphemeralDeclare(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	c1, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c1.Close()

	c2, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c2.Close()

	c1.EphemeralDeclare("e-a")

	payload := Msg([]byte("hello"))

	err = c2.Push("e-a", payload)
	if err != nil {
		panic(err)
	}

	c1.Close()

	err = c2.Push("e-a", payload)
	if err == nil {
		t.Fatal("queue was not deleted")
	}
}
