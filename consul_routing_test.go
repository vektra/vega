package mailbox

import (
	"testing"
	"time"
)

func TestConsulRoutingTable(t *testing.T) {
	m1 := NewMemRegistry()

	ct1, err := NewConsulRoutingTable("127.0.0.1:8899")
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable("127.0.0.1:9900")
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
	serv, err := NewMemService(cPort)
	if err != nil {
		panic(err)
	}

	defer serv.Close()

	cp := &consulPusher{nil, cPort}

	cp.Connect()

	cp.Declare("a")

	payload := Msg([]byte("hello"))

	cp.Push("a", payload)

	msg, err := cp.Poll("a")

	if msg == nil || !msg.Equal(payload) {
		t.Fatal("couldn't talk to the service")
	}
}

func TestConsulRoutingTableWithMultipleDeclares(t *testing.T) {
	m1 := NewMemRegistry()
	m2 := NewMemRegistry()

	ct1, err := NewConsulRoutingTable("127.0.0.1:8899")
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable("127.0.0.1:9900")
	if err != nil {
		panic(err)
	}

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable("127.0.0.1:9911")
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

	mp, ok := pusher.(*multiPusher)
	if !ok {
		t.Fatal("multiPusher not returned")
	}

	if len(mp.pushers) != 2 {
		t.Fatal("pusher doesn't have 2 servers")
	}

	cp1 := mp.pushers[0].(*consulPusher)
	cp2 := mp.pushers[1].(*consulPusher)

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
	m1 := NewMemRegistry()
	m2 := NewMemRegistry()

	ct1, err := NewConsulRoutingTable("127.0.0.1:8899")
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable("127.0.0.1:9900")
	if err != nil {
		panic(err)
	}

	defer ct2.Cleanup()

	ct3, err := NewConsulRoutingTable("127.0.0.1:9911")
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

	ct4, err := NewConsulRoutingTable("127.0.0.1:9922")
	if err != nil {
		panic(err)
	}

	defer ct4.Cleanup()

	m3 := NewMemRegistry()

	ct4.Set("a", m3)

	// propogation delay.
	time.Sleep(100 * time.Millisecond)

	p2, ok := ct3.Get("a")

	if p2 == pusher {
		t.Fatal("cache did not update")
	}

	mp, ok := p2.(*multiPusher)
	if !ok {
		t.Fatal("multiPusher not returned")
	}

	if len(mp.pushers) != 3 {
		t.Fatal("pusher doesn't have 3 servers")
	}
}

func TestConsulRoutingTableRemove(t *testing.T) {
	m1 := NewMemRegistry()

	ct1, err := NewConsulRoutingTable("127.0.0.1:8899")
	if err != nil {
		panic(err)
	}

	defer ct1.Cleanup()

	ct2, err := NewConsulRoutingTable("127.0.0.1:9900")
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
