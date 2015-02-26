package cluster

import (
	"testing"
	"time"

	"github.com/armon/consul-api"
	"github.com/stretchr/testify/require"
	"github.com/vektra/vega"
)

const testRoutingPrefix = "test-mailbox-routing"

func TestConsulRoutingTable(t *testing.T) {
	m1 := vega.NewMemRegistry()

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	require.NoError(t, err)

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", client)
	require.NoError(t, err)

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", client)
	require.NoError(t, err)

	ct1.Set("a", m1)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	pusher, ok := ct2.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	hp := pusher.(*hybridPusher)

	cp := hp.remote[0]

	if cp.target != "127.0.0.1:8899" {
		t.Fatal("didn't pick up the paths correctly")
	}
}

func TestConsulRoutingTableLocalAndRemote(t *testing.T) {
	m1 := vega.NewMemRegistry()

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	require.NoError(t, err)

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", client)
	require.NoError(t, err)

	defer ct1.Cleanup()

	m2 := vega.NewMemRegistry()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", client)
	require.NoError(t, err)

	ct1.Set("a", m1)
	ct2.Set("a", m2)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	pusher, ok := ct2.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	mp, ok := pusher.(*hybridPusher)
	if !ok {
		t.Fatal("multiPusher not returned")
	}

	if mp.local == nil || len(mp.remote) != 1 {
		t.Fatalf("pusher doesn't have 2 servers: %#v", mp)
	}
}

func TestConsulPusher(t *testing.T) {
	serv, err := vega.NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()
	go serv.Accept()

	cp := &consulPusher{nil, cPort}

	cp.Connect()

	cp.Declare("a")

	payload := vega.Msg([]byte("hello"))

	cp.Push("a", payload)

	msg, err := cp.Poll("a")

	if msg == nil || !msg.Message.Equal(payload) {
		t.Fatal("couldn't talk to the service")
	}
}

func TestConsulRoutingTableWithMultipleDeclares(t *testing.T) {
	m1 := vega.NewMemRegistry()
	m2 := vega.NewMemRegistry()

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	require.NoError(t, err)

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", client)
	require.NoError(t, err)

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", client)
	require.NoError(t, err)

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9911", client)
	require.NoError(t, err)

	defer ct3.Cleanup()

	ct1.Set("a", m1)
	ct2.Set("a", m2)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	pusher, ok := ct3.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	mp, ok := pusher.(*hybridPusher)
	if !ok {
		t.Fatal("hybridPusher not returned")
	}

	if mp.Count() != 2 {
		t.Fatal("pusher doesn't have 2 servers")
	}

	cp1 := mp.remote[0]
	cp2 := mp.remote[1]

	if cp1.target == "127.0.0.1:8899" {
		if cp2.target != "127.0.0.1:9900" {
			t.Fatal("both servers were not returned")
		}
	} else if cp1.target == "127.0.0.1:9900" {
		if cp2.target != "127.0.0.1:8899" {
			t.Fatal("both servers were not returned")
		}
	} else {
		t.Fatal("garbage addresses returned")
	}
}

func TestConsulRoutingTableWithMultiPicksUpChanges(t *testing.T) {
	m1 := vega.NewMemRegistry()
	m2 := vega.NewMemRegistry()

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	require.NoError(t, err)

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", client)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", client)
	if err != nil {
		panic(err)
	}

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9911", client)
	if err != nil {
		panic(err)
	}

	defer ct3.Cleanup()

	ct1.Set("a", m1)
	ct2.Set("a", m2)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	pusher, ok := ct3.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	checked, ok := ct3.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	if pusher != checked {
		t.Fatal("pusher caching not working")
	}

	ct4, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9922", client)
	if err != nil {
		panic(err)
	}

	defer ct4.Cleanup()

	m3 := vega.NewMemRegistry()

	ct4.Set("a", m3)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	p2, ok := ct3.Get("a")

	mp, ok := p2.(*hybridPusher)
	if !ok {
		t.Fatal("multiPusher not returned")
	}

	if mp.Count() != 3 {
		t.Fatal("pusher doesn't have 3 servers")
	}
}

func TestConsulRoutingTableWithMultiPicksUpRemovals(t *testing.T) {
	m1 := vega.NewMemRegistry()
	m2 := vega.NewMemRegistry()

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	require.NoError(t, err)

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", client)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", client)
	if err != nil {
		panic(err)
	}

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9911", client)
	if err != nil {
		panic(err)
	}

	defer ct3.Cleanup()

	ct1.Set("a", m1)
	ct2.Set("a", m2)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	pusher, ok := ct3.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	checked, ok := ct3.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	if pusher != checked {
		t.Fatal("pusher caching not working")
	}

	ct1.Remove("a")

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	p2, ok := ct3.Get("a")

	hp, ok := p2.(*hybridPusher)
	if !ok {
		t.Fatal("hybridPusher not returned")
	}

	for _, cp := range hp.remote {
		if cp.target == "127.0.0.1:8899" {
			t.Fatal("did not see node removed from routing table")
		}
	}
}

func TestConsulRoutingTableRemove(t *testing.T) {
	m1 := vega.NewMemRegistry()

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	require.NoError(t, err)

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", client)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", client)
	if err != nil {
		panic(err)
	}

	ct1.Set("a", m1)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	_, ok := ct2.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	ct1.Remove("a")

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	ent, ok := ct2.Get("a")
	if ok {
		hp := ent.(*hybridPusher)

		t.Fatalf("remove failed: %#v", hp.remote[0])
	}
}
