package vega

import (
	"bytes"
	"io"
	"runtime"
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
	assert.NoError(t, err, "mailbox was not declared")

	fc.Close()

	fc, err = Dial(cPort)
	if err != nil {
		panic(err)
	}

	err = fc.Push(name, msg)
	assert.NotNil(t, err, "mailbox was not ephemeral")
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

func TestFeatureClientPipe(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.Write([]byte("hello"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	assert.NoError(t, err)

	data := make([]byte, 5)

	_, err = conn.Read(data)
	assert.NoError(t, err)

	assert.Equal(t, data, []byte("hello"))
}

func TestFeatureClientPipeMoreDataThanBuffer(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.Write([]byte("hello"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	assert.NoError(t, err)

	wg.Wait()

	data := make([]byte, 2)

	n, err := conn.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	assert.Equal(t, []byte("he"), data)

	d2 := make([]byte, 3)

	_, err = conn.Read(d2)
	assert.NoError(t, err)

	assert.Equal(t, []byte("llo"), d2)
}

func TestFeatureClientPipeDrainBuffer(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.Write([]byte("hello"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	assert.NoError(t, err)

	wg.Wait()

	data := make([]byte, 2)

	n, err := conn.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	assert.Equal(t, []byte("he"), data)

	d2 := make([]byte, 2)

	n, err = conn.Read(d2)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	assert.Equal(t, []byte("ll"), d2)

	d3 := make([]byte, 1)

	n, err = conn.Read(d3)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)

	assert.Equal(t, []byte("o"), d3)
}

func TestFeatureClientPipePrependBufferInOneCall(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.Write([]byte("hello"))
		conn.Write([]byte("world"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	assert.NoError(t, err)

	wg.Wait()

	data := make([]byte, 2)

	n, err := conn.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	assert.Equal(t, []byte("he"), data)

	d2 := make([]byte, 2)

	n, err = conn.Read(d2)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	assert.Equal(t, []byte("ll"), d2)

	d3 := make([]byte, 6)

	n, err = conn.Read(d3)
	assert.NoError(t, err)
	assert.Equal(t, 6, n)

	assert.Equal(t, []byte("oworld"), d3)
}

func TestFeatureClientPipeReadMultipleMessages(t *testing.T) {
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

	latch := make(chan bool)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.Write([]byte("hello"))
		conn.Write([]byte("world"))
		<-latch
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	assert.NoError(t, err)

	data := make([]byte, 20)

	n, err := conn.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)

	assert.Equal(t, []byte("helloworld"), data[:n])

	latch <- true

	wg.Wait()
}

func TestFeatureClientPipeReadMultipleMessagesThenClose(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.Write([]byte("hello"))
		conn.Write([]byte("world"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	assert.NoError(t, err)

	wg.Wait()

	data := make([]byte, 20)

	n, err := conn.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)

	assert.Equal(t, []byte("helloworld"), data[:n])
}

func TestFeatureClientPipeReadDeadline(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		time.Sleep(100 * time.Millisecond)
		conn.Write([]byte("1"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	defer conn.Close()

	assert.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))

	data := make([]byte, 1)

	n, err := conn.Read(data)
	assert.Equal(t, 0, n)
	assert.Equal(t, ETimeout, err)

	wg.Wait()
}

func TestFeatureClientPipeCloseAbandonsMailbox(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.Write([]byte("hello"))
		conn.Write([]byte("world"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	assert.NoError(t, err)

	wg.Wait()

	conn.Close()

	err = fc.Push(conn.ownM, Msg("test"))
	assert.Error(t, err)
}

func TestFeatureClientPipeSendBulk(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		n, err := conn.SendBulk(bytes.NewReader([]byte("1")))
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	defer conn.Close()

	assert.NoError(t, err)

	data := make([]byte, 1)

	n, err := conn.Read(data)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte("1"), data)

	wg.Wait()
}

func TestFeatureClientPipeSendBulkBuffered(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.SendBulk(bytes.NewReader([]byte("hello")))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	defer conn.Close()

	assert.NoError(t, err)

	data := make([]byte, 1)

	n, err := conn.Read(data)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte("h"), data)

	d2 := make([]byte, 5)

	n, err = conn.Read(d2)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("ello"), d2[:n])

	wg.Wait()
}

func TestFeatureClientPipeSendBulkSwitchesBack(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		conn.SendBulk(bytes.NewReader([]byte("hello")))
		conn.Write([]byte("world"))
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc.ConnectPipe("a")
	defer conn.Close()

	assert.NoError(t, err)

	data := make([]byte, 1)

	n, err := conn.Read(data)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte("h"), data)

	d2 := make([]byte, 5)

	n, err = conn.Read(d2)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("ello"), d2[:n])

	n, err = conn.Read(d2)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("world"), d2)

	wg.Wait()
}

func TestFeatureClientPipeSendBulkEncrypted(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := fc.ListenPipe("a")
		n, err := conn.SendBulk(bytes.NewReader([]byte("hello")))
		assert.NoError(t, err)
		assert.Equal(t, 5, n)
		conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc2.ConnectPipe("a")
	defer conn.Close()

	assert.NoError(t, err)

	data := make([]byte, 1)

	n, err := conn.Read(data)
	assert.Equal(t, 1, n)
	assert.Equal(t, []byte("h"), data)

	d2 := make([]byte, 4)

	n, err = conn.bulk.(*streamWrapper).Conn.Read(d2)
	assert.Equal(t, 4, n)
	assert.NotEqual(t, []byte("ello"), d2)

	wg.Wait()
}

func TestFeatureClientPipeDetectsClosure(t *testing.T) {
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		lp, _ := fc.ListenPipe("a")
		lp.Write([]byte("hello"))
		fc.Client.conn.Close()
	}()

	runtime.Gosched()

	conn, err := fc2.ConnectPipe("a")
	assert.NoError(t, err)

	data := make([]byte, 5)

	_, err = conn.Read(data)
	assert.NoError(t, err)

	done := make(chan error)

	go func() {
		debugf("read after closed\n")
		_, err := conn.Read(data)
		done <- err
	}()

	select {
	case <-time.Tick(1 * time.Second):
		t.Fatal("read did not detect closure of other side")
	case err = <-done:
		assert.Equal(t, err, io.EOF)
	}
}
