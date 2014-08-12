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

	if !msg.Message.Equal(payload) {
		t.Fatal("body was corrupted")
	}
}

func TestServiceAck(t *testing.T) {
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

	payload := Msg([]byte("hello"))

	c1.Push("a", payload)

	del, err := c1.Poll("a")
	if err != nil {
		panic(err)
	}

	if del == nil {
		t.Fatal("didn't find a message")
	}

	err = del.Ack()
	if err != nil {
		panic(err)
	}

	err = c1.Close()
	if err != nil {
		panic(err)
	}

	c2, err := NewClient(cPort)

	del2, err := c2.Poll("a")
	if err != nil {
		panic(err)
	}

	if del2 != nil {
		t.Fatal("ack did not work")
	}
}

func TestServiceNack(t *testing.T) {
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

	payload := Msg([]byte("hello"))

	c1.Push("a", payload)

	del, err := c1.Poll("a")
	if err != nil {
		panic(err)
	}

	if del == nil {
		t.Fatal("didn't find a message")
	}

	err = del.Nack()
	if err != nil {
		panic(err)
	}

	del2, err := c1.Poll("a")
	if del2 == nil {
		t.Fatal("nack did not return the message")
	}

	if !del.Message.Equal(del2.Message) {
		t.Fatal("nack did not return the proper message")
	}

	stats, err := c1.Stats()
	if err != nil {
		panic(err)
	}

	if stats.InFlight != 1 {
		t.Fatal("inflight stats not incremented")
	}

	err = del2.Nack()
	if err != nil {
		panic(err)
	}

	stats, err = c1.Stats()
	if err != nil {
		panic(err)
	}

	if stats.InFlight != 0 {
		t.Fatalf("nack did not remove the message inflight: %d", stats.InFlight)
	}

	err = del2.Nack()
	if err == nil {
		t.Fatal("did not return double nack error")
	}
}

func TestServiceClientDisconnectsAutoNack(t *testing.T) {
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

	c1.Declare("a")

	payload := Msg([]byte("hello"))

	c1.Push("a", payload)

	del, err := c1.Poll("a")
	if err != nil {
		panic(err)
	}

	if del == nil {
		t.Fatal("didn't find a message")
	}

	err = c1.Close()
	if err != nil {
		panic(err)
	}

	c1, err = NewClient(cPort)
	if err != nil {
		panic(err)
	}

	del2, err := c1.Poll("a")
	if err != nil {
		panic(err)
	}

	if del2 == nil || !del2.Message.Equal(del.Message) {
		t.Fatal("message was not returned again")
	}
}

func TestServiceClientNetworkDisconnectsAutoNack(t *testing.T) {
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

	c1.Declare("a")

	payload := Msg([]byte("hello"))

	c1.Push("a", payload)

	del, err := c1.Poll("a")
	if err != nil {
		panic(err)
	}

	if del == nil {
		t.Fatal("didn't find a message")
	}

	c1.conn.Close()

	// Give the other time to detect the closure.
	// WARNING this is a fragile test and depends on TCP timeouts!
	time.Sleep(100 * time.Millisecond)

	c2, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	del2, err := c2.Poll("a")
	if err != nil {
		panic(err)
	}

	if del2 == nil || !del2.Message.Equal(del.Message) {
		t.Fatal("message was not returned again")
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

	var got *Delivery

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		got, _ = c1.LongPoll("a", 2*time.Second)
	}()

	payload := Msg([]byte("hello"))

	c2.Push("a", payload)

	wg.Wait()

	if got == nil || !got.Message.Equal(payload) {
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

	if !got.Message.Equal(payload) {
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
