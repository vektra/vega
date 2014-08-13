package vega

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func TestFeatureClientAutoEphemeralDeclare(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	fc, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	name := "a#ephemeral"

	fc.Declare(name)

	msg := Msg([]byte("hello"))

	err = fc.Push(name, msg)
	if err != nil {
		t.Fatal("queue was not declared properly")
	}

	fc.Close()

	fc, err = Dial(cPort)
	if err != nil {
		panic(err)
	}

	err = fc.Push(name, msg)
	if err == nil {
		t.Fatal("queue was not ephemeral")
	}
}

func TestFeatureClientReceiveChannel(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	fc, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	fc.Declare("a")

	msg := Msg("hello")

	fc.Push("a", msg)

	rc := fc.Receive("a")
	defer rc.Close()

	select {
	case got := <-rc.Channel:
		if !got.Message.Equal(msg) {
			t.Fatal("got the wrong message")
		}
	case <-time.Tick(1 * time.Second):
		t.Fatal("channel didn't provide a value")
	}
}

func TestFeatureClientReceiveChannelProvidesManyValues(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	fc, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	defer fc.Close()

	fc.Declare("a")

	var messages []*Message

	rc := fc.Receive("a")
	defer rc.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-rc.Channel:
				if msg == nil {
					return
				}

				messages = append(messages, msg.Message)

				if len(messages) == 3 {
					return
				}

			case <-time.Tick(1 * time.Second):
				return
			}
		}
	}()

	msg := Msg("hello")

	fc.Push("a", msg)
	fc.Push("a", msg)
	fc.Push("a", msg)

	wg.Wait()

	if len(messages) != 3 {
		t.Fatalf("channel didn't get all the messages: got %d", len(messages))
	}
}

func TestFeatureClientRequestReply(t *testing.T) {
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	fc, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	defer fc.Close()

	fc2, err := Dial(cPort)
	if err != nil {
		panic(err)
	}

	defer fc2.Close()

	fc.Declare("a")

	go fc.HandleRequests("a", HandlerFunc(func(req *Message) *Message {
		return Msg("hey!")
	}))

	resp, err := fc2.Request("a", Msg("hello"))
	if err != nil {
		panic(err)
	}

	if !bytes.Equal(resp.Message.Body, []byte("hey!")) {
		t.Fatal("didn't get the response")
	}
}
