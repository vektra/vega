package vega

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRegistryPoll(t *testing.T) {
	r := NewMemRegistry()

	v, _ := r.Poll("a")

	assert.Nil(t, v)

	msg := Msg([]byte("hello"))

	r.Declare("a")
	r.Push("a", msg)

	ret, _ := r.Poll("a")

	assert.NotNil(t, ret)
	assert.True(t, msg.Equal(ret.Message))
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

	assert.NotNil(t, got)
	assert.True(t, msg.Equal(got.Message))
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
		assert.Nil(t, got)
	case <-time.Tick(1 * time.Second):
		t.Fatal("long poll did not cancel")
	}

	wg.Wait()

	try, err := r.Poll("a")
	if err != nil {
		panic(err)
	}

	assert.NotNil(t, try)
	assert.True(t, msg.Equal(try.Message))
}
