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
	go serv.Accept()

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
