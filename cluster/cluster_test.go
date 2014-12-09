package cluster

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/vega"
)

const cPort = "127.0.0.1:34002"
const cPort2 = "127.0.0.1:34003"

func TestClusterBadPath(t *testing.T) {
	_, err := NewMemClusterNode("/not/there/i/promise")
	assert.Error(t, err, "did not report an error about invalid path")
}

func TestClusterRegistry(t *testing.T) {
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

	assert.NotNil(t, cn.Registry())
}

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

	payload := vega.Msg([]byte("hello"))

	err = cn.Push("a", payload)
	if err != nil {
		panic(err)
	}

	msg, err := cn.disk.Mailbox("a").Poll()

	assert.Equal(t, payload, msg, "message was not stored locally")
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

	memReg := vega.NewMemRegistry()
	memReg.Declare("a")

	cn.AddRoute("a", memReg)

	payload := vega.Msg([]byte("hello"))

	cn.Push("a", payload)

	msg, err := memReg.Poll("a")

	assert.Equal(t, payload, msg.Message, "message was not stored locally")
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

	msg := vega.Msg([]byte("hello"))

	var got *vega.Delivery

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		got, _ = cn.LongPoll("a", 2*time.Second)
	}()

	cn.Declare("a")
	cn.Push("a", msg)

	wg.Wait()

	assert.Equal(t, msg, got.Message, "long poll didn't see the value")
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

	s1, err := vega.NewService(cPort, cn)
	if err != nil {
		panic(err)
	}

	defer s1.Close()
	go s1.Accept()

	s2, err := vega.NewService(cPort2, cn2)
	if err != nil {
		panic(err)
	}

	defer s2.Close()
	go s2.Accept()

	// Wire up a client going to s1

	toS1, err := vega.NewClient(cPort)
	if err != nil {
		panic(err)
	}

	toS1.Declare("a")
	cn2.AddRoute("a", toS1)

	// Push data into cn2 and see it show up in cn

	toS2, err := vega.NewClient(cPort2)
	if err != nil {
		panic(err)
	}

	msg := vega.Msg([]byte("between nodes"))

	err = toS2.Push("a", msg)
	if err != nil {
		panic(err)
	}

	// debugf("polling\n")

	ret, err := toS1.Poll("a")
	if err != nil {
		panic(err)
	}

	assert.True(t, msg.Equal(ret.Message), "message did not route properly")
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

	payload := vega.Msg([]byte("hello"))

	err = cn.Push("a", payload)
	if err != nil {
		panic(err)
	}

	err = cn.Abandon("a")
	if err != nil {
		panic(err)
	}

	err = cn.Push("a", payload)
	assert.Error(t, err, "mailbox was not abandoned")
}

func TestClusterPubSub(t *testing.T) {
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
	require.NoError(t, err)

	err = cn.Push(":subscribe", &vega.Message{ReplyTo: "a", CorrelationId: "foo"})
	require.NoError(t, err)

	err = cn.Push(":publish", &vega.Message{CorrelationId: "foo", Body: []byte("hello")})
	require.NoError(t, err)

	msg, err := cn.disk.Mailbox("a").Poll()

	assert.Equal(t, []byte("hello"), msg.Body, "message was not stored locally")
}

func TestClusterPubSubBetweenNodes(t *testing.T) {
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

	s1, err := vega.NewService(cPort, cn)
	if err != nil {
		panic(err)
	}

	defer s1.Close()
	go s1.Accept()

	s2, err := vega.NewService(cPort2, cn2)
	if err != nil {
		panic(err)
	}

	defer s2.Close()
	go s2.Accept()

	// Wire up a client going to s1

	toS1, err := vega.NewClient(cPort)
	if err != nil {
		panic(err)
	}

	toS1.Declare("a")
	err = toS1.Push(":subscribe", &vega.Message{ReplyTo: "a", CorrelationId: "foo"})
	require.NoError(t, err)

	cn2.AddRoute(":publish", toS1)

	// Push data into cn2 and see it show up in cn

	toS2, err := vega.NewClient(cPort2)
	if err != nil {
		panic(err)
	}

	msg := &vega.Message{CorrelationId: "foo", Body: []byte("between nodes")}

	err = toS2.Push(":publish", msg)
	require.NoError(t, err)

	// debugf("polling\n")

	ret, err := toS1.Poll("a")
	if err != nil {
		panic(err)
	}

	require.NotNil(t, ret)

	assert.True(t, msg.Equal(ret.Message), "message did not route properly")
}
