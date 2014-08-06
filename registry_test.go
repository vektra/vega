package mailbox

import (
	"sync"
	"testing"
	"time"
)

func TestRegistryPoll(t *testing.T) {
	r := registry()

	_, ok := r.Poll("a")

	if ok {
		t.Fatal("An empty mailbox has values")
	}

	msg := Msg([]byte("hello"))

	r.Declare("a")
	r.Push("a", msg)

	ret, ok := r.Poll("a")

	if !ok {
		t.Fatal("The mailbox did not get the value")
	}

	if !msg.Equal(ret) {
		t.Fatal("Wrong value")
	}
}

func TestLongPollRegistry(t *testing.T) {
	r := registry()

	msg := Msg([]byte("hello"))

	r.Declare("a")

	var got *Message

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

	if !msg.Equal(got) {
		t.Fatal("Wrong value")
	}

}

func TestLongPollRegistryTimeout(t *testing.T) {
	r := registry()

	r.Declare("a")

	var got *Message

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		dur, _ := time.ParseDuration("1s")
		got, _ = r.LongPoll("a", dur)
	}()

	wg.Wait()
}
