package cluster

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektra/vega"
)

func TestConsulNodeConfigDefaults(t *testing.T) {
	cfg := &ConsulNodeConfig{}
	err := cfg.Normalize()
	if err != nil {
		panic(err)
	}

	assert.Equal(t, cfg.ListenPort, vega.DefaultPort)

	ip, err := vega.GetPrivateIP()
	if err != nil {
		panic(err)
	}

	assert.Equal(t, cfg.AdvertiseAddr, ip.String())
	assert.Equal(t, cfg.DataPath, DefaultPath)
}

func TestConsulNode(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn1, err := NewConsulClusterNode(
		&ConsulNodeConfig{
			AdvertiseAddr: "127.0.0.1",
			ListenPort:    8899,
			DataPath:      dir})

	if err != nil {
		panic(err)
	}

	defer cn1.Close()
	go cn1.Accept()

	dir2, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir2)

	cn2, err := NewConsulClusterNode(
		&ConsulNodeConfig{
			AdvertiseAddr: "127.0.0.1",
			ListenPort:    9900,
			DataPath:      dir2})

	if err != nil {
		panic(err)
	}

	defer cn2.Close()
	go cn2.Accept()

	cn1.Declare("a")

	// propagation delay
	time.Sleep(1000 * time.Millisecond)

	msg := vega.Msg([]byte("hello"))

	// debugf("pushing...\n")
	err = cn2.Push("a", msg)
	require.NoError(t, err)

	// debugf("polling...\n")
	got, err := cn1.Poll("a")
	if err != nil {
		panic(err)
	}

	assert.True(t, got.Message.Equal(msg), "didn't get the message")
}

func TestConsulNodeRedeclaresOnStart(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn1, err := NewConsulClusterNode(
		&ConsulNodeConfig{
			AdvertiseAddr: "127.0.0.1",
			ListenPort:    8899,
			DataPath:      dir})

	if err != nil {
		panic(err)
	}

	// defer cn1.Close()
	go cn1.Accept()

	cl, err := vega.NewClient(":8899")

	cn1.Declare("a")

	// propagation delay
	time.Sleep(1000 * time.Millisecond)

	msg := vega.Msg([]byte("hello"))

	// debugf("pushing...\n")
	err = cl.Push("a", msg)
	if err != nil {
		panic(err)
	}

	cl.Close()
	cn1.Cleanup()
	cn1.Close()

	cn1, err = NewConsulClusterNode(
		&ConsulNodeConfig{
			AdvertiseAddr: "127.0.0.1",
			ListenPort:    8899,
			DataPath:      dir})

	if err != nil {
		panic(err)
	}

	defer cn1.Close()
	go cn1.Accept()

	cl, _ = vega.NewClient(":8899")

	err = cl.Push("a", msg)
	assert.NoError(t, err, "routes were not readded")
}

func TestConsulNodePubSubBetweenNodes(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	cn1, err := NewConsulClusterNode(
		&ConsulNodeConfig{
			AdvertiseAddr: "127.0.0.1",
			ListenPort:    8899,
			DataPath:      dir})

	if err != nil {
		panic(err)
	}

	defer cn1.Cleanup()

	defer cn1.Close()
	go cn1.Accept()

	dir2, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir2)

	cn2, err := NewConsulClusterNode(
		&ConsulNodeConfig{
			AdvertiseAddr: "127.0.0.1",
			ListenPort:    9900,
			DataPath:      dir2})

	if err != nil {
		panic(err)
	}

	defer cn2.Cleanup()

	defer cn2.Close()
	go cn2.Accept()

	cn1.Declare("a")

	err = cn1.Push(":subscribe", &vega.Message{ReplyTo: "a", CorrelationId: "foo"})
	require.NoError(t, err)

	// propagation delay
	time.Sleep(1000 * time.Millisecond)

	msg := &vega.Message{CorrelationId: "foo", Body: []byte("between nodes")}

	err = cn2.Push(":publish", msg)
	require.NoError(t, err)

	// debugf("polling\n")

	ret, err := cn1.Poll("a")
	if err != nil {
		panic(err)
	}

	require.NotNil(t, ret)

	assert.True(t, msg.Equal(ret.Message), "message did not route properly")
}
