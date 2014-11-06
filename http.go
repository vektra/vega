package vega

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bmizerany/pat"
	"github.com/ugorji/go/codec"
)

var DefaultHTTPPort = DefaultPort + 2

var ctMsgPack = "application/x-msgpack"
var ctUrlEncoded = "application/x-www-form-urlencoded"

type inflightDelivery struct {
	delivery *Delivery
	expires  time.Time
}

type HTTPService struct {
	Address  string
	Registry *Registry

	listener net.Listener
	server   *http.Server
	mux      *pat.PatternServeMux

	defaultLease time.Duration
	lock         sync.Mutex
	inflight     map[MessageId]*inflightDelivery

	background chan struct{}

	wg   sync.WaitGroup
	done chan struct{}
}

func NewHTTPService(port string, reg *Registry) *HTTPService {
	h := &HTTPService{
		Address:      port,
		Registry:     reg,
		mux:          pat.New(),
		defaultLease: 5 * time.Minute,
		inflight:     make(map[MessageId]*inflightDelivery),
		background:   make(chan struct{}, 3),
		done:         make(chan struct{}),
	}

	h.mux.Post("/mailbox/:name", http.HandlerFunc(h.declare))
	h.mux.Add("DELETE", "/mailbox/:name", http.HandlerFunc(h.abandon))
	h.mux.Put("/mailbox/:name", http.HandlerFunc(h.push))
	h.mux.Get("/mailbox/:name", http.HandlerFunc(h.poll))

	h.mux.Add("DELETE", "/message/:id", http.HandlerFunc(h.ack))
	h.mux.Put("/message/:id", http.HandlerFunc(h.nack))

	s := &http.Server{
		Addr:           port,
		Handler:        h.mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	h.server = s

	return h
}

func (h *HTTPService) CheckTimeouts() {
	h.lock.Lock()

	now := time.Now()

	var toRemove []MessageId

	for id, inf := range h.inflight {
		if inf.expires.Before(now) {
			inf.delivery.Nack()
			toRemove = append(toRemove, id)
		}
	}

	for _, id := range toRemove {
		delete(h.inflight, id)
	}

	h.lock.Unlock()
}

func (h *HTTPService) minimumTimeout() time.Duration {
	h.lock.Lock()
	defer h.lock.Unlock()

	if len(h.inflight) == 0 {
		return h.defaultLease
	}

	var min time.Duration

	now := time.Now()

	for _, inf := range h.inflight {
		t := inf.expires.Sub(now)
		if t <= 0 {
			return t
		}

		if min == 0 {
			min = t
		} else if t < min {
			min = t
		}
	}

	return min
}

func (h *HTTPService) BackgroundTimeouts() {
	h.wg.Add(1)

	go func() {
		defer h.wg.Done()

		var min time.Duration

		for {
			select {
			case <-h.done:
				return
			case <-h.background:
				min = h.minimumTimeout()
			case <-time.Tick(min):
				h.CheckTimeouts()
				min = h.minimumTimeout()
			}
		}
	}()
}

func (h *HTTPService) Listen() error {
	l, err := net.Listen("tcp", h.Address)
	if err != nil {
		return err
	}

	h.listener = l
	return nil
}

func (h *HTTPService) Close() {
	h.lock.Lock()

	close(h.done)

	if h.listener != nil {
		h.listener.Close()
	}

	for _, inf := range h.inflight {
		inf.delivery.Nack()
	}

	h.lock.Unlock()

	h.wg.Wait()
}

func (h *HTTPService) Accept() error {
	return h.server.Serve(&gracefulListener{h.listener, &h.wg})
}

func (h *HTTPService) declare(rw http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get(":name")

	err := h.Registry.Declare(name)
	if err != nil {
		rw.WriteHeader(500)
		rw.Write([]byte(err.Error()))
	}
}

func (h *HTTPService) abandon(rw http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get(":name")

	err := h.Registry.Abandon(name)
	if err != nil {
		rw.WriteHeader(500)
		rw.Write([]byte(err.Error()))
	}
}

func (h *HTTPService) push(rw http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get(":name")

	var msg Message
	var err error

	contentType := req.Header.Get("Content-Type")

	switch contentType {
	case ctMsgPack:
		err = codec.NewDecoder(req.Body, &msgpack).Decode(&msg)
	case ctUrlEncoded:
		data, le := ioutil.ReadAll(req.Body)
		if le != nil {
			err = le
		} else {
			msg.Body = data
		}
	default:
		err = json.NewDecoder(req.Body).Decode(&msg)
	}

	if err != nil {
		rw.WriteHeader(500)
		rw.Write([]byte(err.Error()))
		return
	}

	err = h.Registry.Push(name, &msg)
	if err != nil {
		rw.WriteHeader(500)
		rw.Write([]byte(err.Error()))
	}
}

func (h *HTTPService) poll(rw http.ResponseWriter, req *http.Request) {
	name := req.URL.Query().Get(":name")

	var err error
	var del *Delivery

	wait := req.URL.Query().Get("wait")
	if wait != "" {
		dur, err := time.ParseDuration(wait)

		if err != nil {
			rw.WriteHeader(500)
			rw.Write([]byte(err.Error()))
			return
		}

		del, err = h.Registry.LongPollCancelable(name, dur, h.done)
	} else {
		del, err = h.Registry.Poll(name)
	}

	if err != nil {
		if err == ENoMailbox {
			rw.WriteHeader(404)
		} else {
			rw.WriteHeader(500)
			rw.Write([]byte(err.Error()))
		}
		return
	}

	if del == nil {
		rw.WriteHeader(204)
		return
	}

	if req.Header.Get("Accept") == ctMsgPack {
		err = codec.NewEncoder(rw, &msgpack).Encode(del.Message)
	} else {
		err = json.NewEncoder(rw).Encode(del.Message)
	}

	if err != nil {
		del.Nack()
		rw.WriteHeader(500)
		rw.Write([]byte(err.Error()))
		return
	}

	h.lock.Lock()

	dur := h.defaultLease

	lease := req.URL.Query().Get("lease")
	if lease != "" {
		d, err := time.ParseDuration(lease)
		if err == nil {
			dur = d
		}
	}

	expires := time.Now().Add(dur)

	h.inflight[del.Message.MessageId] = &inflightDelivery{del, expires}

	// wakeup the background if it's there, don't block
	// Side note: these are probably the weirds 4 lines you can write
	// in go.
	select {
	case h.background <- struct{}{}:
	default:
	}

	h.lock.Unlock()
}

func (h *HTTPService) ack(rw http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get(":id")

	var del *inflightDelivery
	var ok bool

	mid := MessageId(id)

	h.lock.Lock()

	del, ok = h.inflight[mid]
	if ok {
		delete(h.inflight, mid)
	}

	h.lock.Unlock()

	if !ok {
		rw.WriteHeader(404)
		return
	}

	err := del.delivery.Ack()

	if err != nil {
		rw.WriteHeader(500)
		rw.Write([]byte(err.Error()))
	}
}

func (h *HTTPService) nack(rw http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get(":id")

	var del *inflightDelivery
	var ok bool

	mid := MessageId(id)

	h.lock.Lock()

	del, ok = h.inflight[mid]
	if ok {
		delete(h.inflight, mid)
	}

	h.lock.Unlock()

	if !ok {
		rw.WriteHeader(404)
		return
	}

	err := del.delivery.Nack()

	if err != nil {
		rw.WriteHeader(500)
		rw.Write([]byte(err.Error()))
	}
}
