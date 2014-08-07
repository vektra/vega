package mailbox

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/vektra/consul_kv_cache/cache"
)

type consulRoutingTable struct {
	selfId []byte
	key    string
	names  []string

	lock sync.RWMutex

	local RouteTable

	consul *cache.ConsulKVCache

	connections map[string]*consulPusher

	cache *map[string]Pusher
}

func NewConsulRoutingTable(id string) (*consulRoutingTable, error) {
	h := sha1.New()
	h.Write([]byte(id))
	k := hex.EncodeToString(h.Sum(nil))

	tbl := make(map[string]Pusher)

	ct := &consulRoutingTable{
		selfId:      []byte(id),
		key:         k,
		names:       nil,
		local:       make(MemRouteTable),
		consul:      cache.NewConsulKVCache("mailbox-routing"),
		cache:       &tbl,
		connections: make(map[string]*consulPusher),
	}

	return ct, nil
}

var bTrue = []byte("true\n")

func setConsulKV(key string, value []byte) error {
	url := "http://localhost:8500/v1/kv/" + key

	body := bytes.NewReader(value)

	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return err
	}

	req.ContentLength = int64(len(value))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)

	if !bytes.Equal(data, bTrue) {
		errors.New("Consul returned an error setting the value")
	}

	return nil
}

func delConsulKV(key string, rec bool) error {
	url := "http://localhost:8500/v1/kv/" + key

	if rec {
		url += "?recurse"
	}

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	resp.Body.Close()

	return nil
}

func (ct *consulRoutingTable) Set(name string, p Pusher) error {
	ct.local.Set(name, p)

	url := "http://localhost:8500/v1/kv/mailbox-routing/" + name + "/" + ct.key

	body := bytes.NewReader(ct.selfId)

	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		log.Fatalf("Error creating http req: %s, %#v\n", err, req)
	}

	req.ContentLength = int64(len(ct.selfId))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Error sending PUT req: %#v\n", resp)
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)

	if !bytes.Equal(data, bTrue) {
		log.Fatalf("Consul returned an error setting the value")
	}

	ct.names = append(ct.names, name)
	return nil
}

type consulPusher struct {
	client Storage
	target string
}

type consulValue struct {
	CreateIndex int
	ModifyIndex int
	Key         string
	Flags       int
	Value       []byte
}

func (ct *consulRoutingTable) Get(name string) (Pusher, bool) {
	if lp, ok := ct.local.Get(name); ok {
		return lp, true
	}

	url := "http://localhost:8500/v1/kv/mailbox-routing/" + name + "?recurse"

	resp, err := http.Get(url)
	if err != nil {
		return nil, false
	}

	defer resp.Body.Close()

	var values []consulValue

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&values)
	if err != nil && err != io.EOF {
		return nil, false
	}

	if len(values) == 0 {
		return nil, false
	}

	if len(values) == 1 {
		id := string(values[0].Value)

		if cp, ok := ct.connections[id]; ok {
			return cp, true
		}

		cp := &consulPusher{nil, string(values[0].Value)}

		ct.connections[id] = cp

		cp.Connect()

		return cp, true
	}

	mp := NewMultiPusher()

	for _, val := range values {
		id := string(val.Value)

		if cp, ok := ct.connections[id]; ok {
			mp.Add(cp)
		} else {
			cp := &consulPusher{nil, id}

			ct.connections[id] = cp

			cp.Connect()

			mp.Add(cp)
		}
	}

	return mp, true
}

func (ct *consulRoutingTable) CacheGet(name string) (Pusher, bool) {
	ptr := unsafe.Pointer(ct.cache)
	tbl := atomic.LoadPointer(&ptr)
	cp, ok := (*((*map[string]Pusher)(tbl)))[name]

	return cp, ok
}

func (ct *consulRoutingTable) addEntry(tbl map[string]Pusher, val *consulValue) {
	cp := &consulPusher{nil, string(val.Value)}

	start := strings.Index(val.Key, "mailbox-routing/") + len("mailbox-routing/")

	end := strings.LastIndex(val.Key, "/")

	name := val.Key[start:end]

	debugf("added to the consul route cache: %s => %s\n", name, string(val.Value))

	cp.Connect()

	if pusher, ok := tbl[name]; ok {
		switch st := pusher.(type) {
		case *consulPusher:
			mp := NewMultiPusher()
			mp.Add(st)
			mp.Add(cp)

			tbl[name] = mp
		case *multiPusher:
			st.Add(cp)
		default:
			panic("unknown pusher type?")
		}
	} else {
		tbl[name] = cp
	}
}

func (ct *consulRoutingTable) primeCache() (int, error) {
	url := "http://localhost:8500/v1/kv/mailbox-routing/?recurse"

	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}

	defer resp.Body.Close()

	var values []consulValue

	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&values)
	if err != nil && err != io.EOF {
		return 0, err
	}

	tbl := make(map[string]Pusher)

	for _, val := range values {
		ct.addEntry(tbl, &val)
	}

	fmt.Printf("built table: %#v\n", tbl)

	idx, _ := strconv.Atoi(resp.Header.Get("X-Consul-Index"))

	cur := unsafe.Pointer(ct.cache)

	atomic.StorePointer(&cur, unsafe.Pointer(&tbl))

	return idx, nil
}

func (ct *consulRoutingTable) updateCache(idx int) {
	url := "http://localhost:8500/v1/kv/mailbox-routing/?recurse"

	for {
		resp, err := http.Get(fmt.Sprintf("%s&index=%d&wait=5m", url, idx))
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		defer resp.Body.Close()

		var values []consulValue

		dec := json.NewDecoder(resp.Body)
		err = dec.Decode(&values)
		if err != nil && err != io.EOF {
			time.Sleep(1 * time.Second)
			continue
		}

		if len(values) == 0 {
			continue
		}

		// Because consul does not send watches for deletes, when we detect
		// a change, we have to reread all the routes.
		ct.primeCache()

		idx, _ = strconv.Atoi(resp.Header.Get("X-Consul-Index"))
	}
}

func (ct *consulRoutingTable) Cleanup() error {
	for _, name := range ct.names {
		url := "http://localhost:8500/v1/kv/mailbox-routing/" + name + "/" + ct.key

		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		resp.Body.Close()
	}

	return nil
}

func (cp *consulPusher) Connect() error {
	c, err := Dial(cp.target)
	if err != nil {
		return err
	}

	cp.client = NewReliableStorage(c)

	return nil
}

func (cp *consulPusher) Declare(name string) error {
	if cp.client == nil {
		err := cp.Connect()
		if err != nil {
			return err
		}
	}

	return cp.client.Declare(name)
}

func (cp *consulPusher) Push(name string, msg *Message) error {
	if cp.client == nil {
		err := cp.Connect()
		if err != nil {
			return err
		}
	}

	return cp.client.Push(name, msg)
}

func (cp *consulPusher) Poll(name string) (*Message, error) {
	if cp.client == nil {
		err := cp.Connect()
		if err != nil {
			return nil, err
		}
	}

	return cp.client.Poll(name)
}
