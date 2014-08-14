package vega

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err, "queue was not declared")

	fc.Close()

	fc, err = Dial(cPort)
	if err != nil {
		panic(err)
	}

	err = fc.Push(name, msg)
	assert.NotNil(t, err, "queue was not ephemeral")
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
		assert.True(t, msg.Equal(got.Message), "wrong message")
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

	assert.Equal(t, len(messages), 3, "channel didn't get 3 messages")
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

	assert.True(t, bytes.Equal(resp.Message.Body, []byte("hey!")), "wrong message")
}
