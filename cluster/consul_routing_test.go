package cluster

import (
	"testing"
	"time"

	"github.com/vektra/vega"
)

const testRoutingPrefix = "test-mailbox-routing"

func TestConsulRoutingTable(t *testing.T) {
	m1 := vega.NewMemRegistry()

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", nil)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", nil)
	if err != nil {
		panic(err)
	}

	ct1.Set("a", m1)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	pusher, ok := ct2.Get("a")
	if !ok {
		t.Fatal("couldn't find a")
	}

	cp := pusher.(*consulPusher)

	if cp.target != "127.0.0.1:8899" {
		t.Fatal("didn't pick up the paths correctly")
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

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", nil)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", nil)
	if err != nil {
		panic(err)
	}

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9911", nil)
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

	mp, ok := pusher.(*vega.MultiPusher)
	if !ok {
		t.Fatal("multiPusher not returned")
	}

	if len(mp.Pushers) != 2 {
		t.Fatal("pusher doesn't have 2 servers")
	}

	cp1 := mp.Pushers[0].(*consulPusher)
	cp2 := mp.Pushers[1].(*consulPusher)

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

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", nil)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", nil)
	if err != nil {
		panic(err)
	}

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9911", nil)
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

	ct4, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9922", nil)
	if err != nil {
		panic(err)
	}

	defer ct4.Cleanup()

	m3 := vega.NewMemRegistry()

	ct4.Set("a", m3)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	p2, ok := ct3.Get("a")

	if p2 == pusher {
		t.Fatal("cache did not update")
	}

	mp, ok := p2.(*vega.MultiPusher)
	if !ok {
		t.Fatal("multiPusher not returned")
	}

	if len(mp.Pushers) != 3 {
		t.Fatal("pusher doesn't have 3 servers")
	}
}

func TestConsulRoutingTableWithMultiPicksUpRemovals(t *testing.T) {
	m1 := vega.NewMemRegistry()
	m2 := vega.NewMemRegistry()

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", nil)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", nil)
	if err != nil {
		panic(err)
	}

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9911", nil)
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

	if p2 == pusher {
		t.Fatal("cache did not update")
	}

	_, ok = p2.(*consulPusher)
	if !ok {
		t.Fatal("multiPusher not returned")
	}
}

func TestConsulRoutingTableRemove(t *testing.T) {
	m1 := vega.NewMemRegistry()

	ct1, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:8899", nil)
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable(testRoutingPrefix, "127.0.0.1:9900", nil)
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

	_, ok = ct2.Get("a")
	if ok {
		t.Fatal("remove failed")
	}
}
