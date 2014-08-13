package mailbox

import (
	"sync"
	"testing"
	"time"
)

func TestRegistryPoll(t *testing.T) {
	r := NewMemRegistry()

	v, _ := r.Poll("a")

	if v != nil {
		t.Fatal("An empty mailbox has values")
	}

	msg := Msg([]byte("hello"))

	r.Declare("a")
	r.Push("a", msg)

	ret, _ := r.Poll("a")

	if ret == nil {
		t.Fatal("The mailbox did not get the value")
	}

	if !msg.Equal(ret.Message) {
		t.Fatal("Wrong value")
	}
}

func TestLongPollRegistry(t *testing.T) {
	r := NewMemRegistry()

	msg := Msg([]byte("hello"))

	r.Declare("a")

	var got *Delivery

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		dur, _ := time.ParseDuration("2s")
		got, _ = r.LongPoll("a", dur)
	}()

	time.Sleep(500 * time.Millisecond)

	r.Push("a", msg)

	wg.Wait()

	if !msg.Equal(got.Message) {
		t.Fatal("Wrong value")
	}

}

func TestLongPollRegistryTimeout(t *testing.T) {
	r := NewMemRegistry()

	r.Declare("a")

	var got *Delivery

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		dur, _ := time.ParseDuration("1s")
		got, _ = r.LongPoll("a", dur)
	}()

	wg.Wait()
}

func TestLongPollRegistryIsCancelable(t *testing.T) {
	r := NewMemRegistry()

	msg := Msg([]byte("hello"))

	r.Declare("a")

	done := make(chan struct{})

	var wg sync.WaitGroup

	fin := make(chan *Delivery)

	wg.Add(1)
	go func() {
		defer wg.Done()

		dur, _ := time.ParseDuration("2s")
		got, _ := r.LongPollCancelable("a", dur, done)
		fin <- got
	}()

	time.Sleep(100 * time.Millisecond)

	close(done)

	r.Push("a", msg)

	select {
	case got := <-fin:
		if got != nil {
			t.Fatal("cancelled long poll returned... something")
		}
	case <-time.Tick(1 * time.Second):
		t.Fatal("long poll did not cancel")
	}

	wg.Wait()

	try, err := r.Poll("a")
	if err != nil {
		panic(err)
	}

	if try == nil || !msg.Equal(try.Message) {
		t.Fatal("Wrong value")
	}

}
