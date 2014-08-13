package vega

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ugorji/go/codec"
)

func TestHTTPDeclareMailbox(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatal("server had an error")
	}

	err = reg.Push("a", Msg("hello"))
	if err != nil {
		t.Fatal("queue was not created")
	}
}

func TestHTTPAbandonMailbox(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatal("server had an error")
	}

	err = reg.Push("a", Msg("hello"))
	if err == nil {
		t.Fatal("mailbox was not abandon")
	}
}

func TestHTTPPushMailbox(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	body, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("PUT", url, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	del, err := reg.Poll("a")

	if del == nil || !del.Message.Equal(msg) {
		t.Fatal("message not pushed")
	}
}

func TestHTTPPushMailboxMsgPack(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	var body []byte
	err := codec.NewEncoderBytes(&body, &msgpack).Encode(msg)
	if err != nil {
		panic(err)
	}

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("PUT", url, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Content-Type", ctMsgPack)

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	del, err := reg.Poll("a")

	if del == nil || !del.Message.Equal(msg) {
		t.Fatal("message not pushed")
	}
}

func TestHTTPPollMailbox(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	reg.Push("a", msg)

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	var ret Message

	err = json.NewDecoder(rw.Body).Decode(&ret)
	if err != nil {
		panic(err)
	}

	if !ret.Equal(msg) {
		t.Fatal("poll did not return the message")
	}
}

func TestHTTPPollMailboxMsgPack(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	reg.Push("a", msg)

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	req.Header.Add("Accept", ctMsgPack)

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	var ret Message

	err = codec.NewDecoder(rw.Body, &msgpack).Decode(&ret)
	if err != nil {
		panic(err)
	}

	if !ret.Equal(msg) {
		t.Fatal("poll did not return the message")
	}
}

func TestHTTPPollEmptyMailbox(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 204 {
		t.Fatalf("server didn't return 204: %d", rw.Code)
	}
}

func TestHTTPPollMailboxWithWait(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	url := fmt.Sprintf("http://%s/mailbox/a?wait=10s", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	done := make(chan struct{})

	go func() {
		serv.mux.ServeHTTP(rw, req)
		close(done)
	}()

	// To be sure it actually waits
	time.Sleep(100 * time.Millisecond)

	msg := Msg("hello")

	reg.Push("a", msg)

	select {
	case <-time.Tick(1 * time.Second):
		t.Fatalf("didn't get the message in time")

	case <-done:
		if rw.Code != 200 {
			t.Fatalf("server had an error: %d", rw.Code)
		}

		var ret Message

		err = json.NewDecoder(rw.Body).Decode(&ret)
		if err != nil {
			panic(err)
		}

		if !ret.Equal(msg) {
			t.Fatal("poll did not return the message")
		}
	}
}

func TestHTTPAckMessage(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	reg.Push("a", msg)

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	var ret Message

	err = json.NewDecoder(rw.Body).Decode(&ret)
	if err != nil {
		panic(err)
	}

	// Now ack it.

	url = fmt.Sprintf("http://%s/message/%s", cPort, ret.MessageId)

	req, err = http.NewRequest("DELETE", url, nil)
	if err != nil {
		panic(err)
	}

	rw = httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	if len(serv.inflight) != 0 {
		t.Fatalf("message was not removed from inflight")
	}
}

func TestHTTPNackMessage(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	reg.Push("a", msg)

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	var ret Message

	err = json.NewDecoder(rw.Body).Decode(&ret)
	if err != nil {
		panic(err)
	}

	// Now ack it.

	url = fmt.Sprintf("http://%s/message/%s", cPort, ret.MessageId)

	req, err = http.NewRequest("PUT", url, nil)
	if err != nil {
		panic(err)
	}

	rw = httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	if len(serv.inflight) != 0 {
		t.Fatalf("message was not removed from inflight")
	}

	del, err := reg.Poll("a")
	if err != nil {
		panic(err)
	}

	if del == nil {
		t.Fatal("nack did not return the message")
	}
}

func TestHTTPCheckTimeoutsNacks(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	reg.Push("a", msg)

	url := fmt.Sprintf("http://%s/mailbox/a?lease=1s", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	time.Sleep(1 * time.Second)

	serv.CheckTimeouts()

	// Now see if it's back available

	url = fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw = httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}
}

func TestHTTPAutoNackAfterTimeout(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	serv.BackgroundTimeouts()

	reg.Declare("a")

	msg := Msg("hello")

	reg.Push("a", msg)

	url := fmt.Sprintf("http://%s/mailbox/a?lease=1s", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	time.Sleep(2 * time.Second)

	// Now see if it's back available

	url = fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err = http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw = httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	// Check that the 2nd poll got the default lease
	serv.CheckTimeouts()

	if len(serv.inflight) != 1 {
		t.Fatal("server didn't nack the message properly")
	}
}

func TestHTTPShutdownAutoNacks(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	msg := Msg("hello")

	reg.Push("a", msg)

	url := fmt.Sprintf("http://%s/mailbox/a", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	serv.mux.ServeHTTP(rw, req)

	if rw.Code != 200 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

	serv.Close()

	del, err := reg.Poll("a")
	if err != nil {
		panic(err)
	}

	if del == nil {
		t.Fatal("shutdown did not nack inflight messages")
	}
}

func TestHTTPShutdownHandlesLongPoll(t *testing.T) {
	reg := NewMemRegistry()
	serv := NewHTTPService(cPort, reg)

	reg.Declare("a")

	url := fmt.Sprintf("http://%s/mailbox/a?wait=1m", cPort)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	rw := httptest.NewRecorder()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		serv.mux.ServeHTTP(rw, req)
	}()

	time.Sleep(100 * time.Millisecond)

	serv.Close()

	// simulate a message coming in elsewhere while shutting down

	msg := Msg("hello")

	reg.Push("a", msg)

	time.Sleep(100 * time.Millisecond)

	del, err := reg.Poll("a")
	if err != nil {
		panic(err)
	}

	if del == nil {
		t.Fatal("shutdown did not nack inflight messages")
	}

	wg.Wait()

	if rw.Code != 204 {
		t.Fatalf("server had an error: %d", rw.Code)
	}

}
