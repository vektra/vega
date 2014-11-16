package vega

import (
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cPort = "127.0.0.1:34000"
const cPort2 = "127.0.0.1:34001"

func init() {
	muxConfig.LogOutput = ioutil.Discard
}

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
	assert.Nil(t, msg)

	payload := Msg([]byte("hello"))

	c2.Push("a", payload)

	msg, err = c2.Poll("a")
	if err != nil {
		panic(err)
	}

	assert.True(t, payload.Equal(msg.Message))
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

	assert.NotNil(t, del)

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

	assert.Nil(t, del2)
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

	assert.NotNil(t, del)

	err = del.Nack()
	if err != nil {
		panic(err)
	}

	del2, err := c1.Poll("a")
	assert.NotNil(t, del2)

	assert.True(t, del.Message.Equal(del2.Message))

	stats, err := c1.Stats()
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 1, stats.InFlight)

	err = del2.Nack()
	if err != nil {
		panic(err)
	}

	stats, err = c1.Stats()
	if err != nil {
		panic(err)
	}

	assert.Equal(t, 0, stats.InFlight)

	err = del2.Nack()
	assert.Error(t, err)
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

	assert.NotNil(t, del)

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

	assert.True(t, del2.Message.Equal(del.Message))
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

	assert.NotNil(t, del)

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

	assert.True(t, del.Message.Equal(del2.Message))
}

func TestServiceShutdownAutoNack(t *testing.T) {
	reg := NewMemRegistry()
	serv, err := NewService(cPort, reg)
	if err != nil {
		panic(err)
	}

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

	assert.NotNil(t, del)

	serv.Close()

	serv, err = NewService(cPort, reg)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	c2, err := NewClient(cPort)
	if err != nil {
		panic(err)
	}

	defer c2.Close()

	del2, err := c2.Poll("a")
	if err != nil {
		panic(err)
	}

	assert.True(t, del.Message.Equal(del2.Message))
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

	assert.NotNil(t, got)
	assert.True(t, payload.Equal(got.Message))
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
	assert.NoError(t, err)

	got, err := c1.Poll("a")

	assert.True(t, payload.Equal(got.Message))
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
	assert.NoError(t, err)

	err = c1.Abandon("a")
	if err != nil {
		panic(err)
	}

	err = c2.Push("a", payload)
	assert.Error(t, err)
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
	assert.Error(t, err)
}

func TestServiceDeclareLWT(t *testing.T) {
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

	msg := &Message{
		ReplyTo: "a",
		Type:    "death",
	}

	c2.Push(":lwt", msg)

	lwt, err := c2.Poll(":lwt")
	require.NoError(t, err)

	assert.Equal(t, lwt.Message, msg)

	lwt, err = c2.LongPoll(":lwt", 1*time.Second)
	require.NoError(t, err)

	assert.Equal(t, lwt.Message, msg)

	debugf("closing c2\n")
	c2.Close()

	got, err := c1.Poll("a")
	require.NoError(t, err)

	assert.Equal(t, "death", got.Message.Type)
}

func TestServiceDeclareLWTOnEphemeral(t *testing.T) {
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
	c2.EphemeralDeclare("e-a")

	msg := &Message{
		ReplyTo:       "a",
		CorrelationId: "e-a",
		Type:          "death",
	}

	c2.Push(":lwt", msg)

	c2.Abandon("e-a")

	got, err := c1.Poll("a")
	require.NoError(t, err)

	require.NotNil(t, got)

	assert.Equal(t, "death", got.Message.Type)
}
