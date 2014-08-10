package mailbox

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"

	"net/http"
	"sync"

	"github.com/vektra/consul_kv_cache/cache"
)

type cachedPusher struct {
	clock  cache.ClockValue
	pusher Pusher
}

type consulRoutingTable struct {
	selfId []byte
	key    string
	names  map[string]bool

	lock sync.RWMutex

	local RouteTable

	consul *cache.ConsulKVCache

	connections map[string]*consulPusher

	cache map[string]*cachedPusher
}

func NewConsulRoutingTable(id string) (*consulRoutingTable, error) {
	h := sha1.New()
	h.Write([]byte(id))
	k := hex.EncodeToString(h.Sum(nil))

	consul := cache.NewConsulKVCache("mailbox-routing")

	go consul.BackgroundUpdate()

	ct := &consulRoutingTable{
		selfId:      []byte(id),
		key:         k,
		names:       make(map[string]bool),
		local:       make(MemRouteTable),
		consul:      consul,
		connections: make(map[string]*consulPusher),
		cache:       make(map[string]*cachedPusher),
	}

	return ct, nil
}

func (ct *consulRoutingTable) Close() {
	ct.consul.Close()
}

func (ct *consulRoutingTable) Set(name string, p Pusher) error {
	err := ct.local.Set(name, p)
	if err != nil {
		return err
	}

	key := name + "/" + ct.key

	err = ct.consul.Set(key, ct.selfId)
	if err != nil {
		return err
	}

	ct.names[name] = true
	return nil
}

func (ct *consulRoutingTable) Remove(name string) error {
	delete(ct.names, name)

	key := name + "/" + ct.key

	ct.consul.Delete(key)
	return ct.local.Remove(name)
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
		debugf("found local pusher for %s: %#v\n", name, lp)
		return lp, true
	}

	values, clock := ct.consul.GetPrefix(name)

	if len(values) == 0 {
		return nil, false
	}

	if cp, ok := ct.cache[name]; ok {
		if cp.clock >= clock {
			debugf("using cached value to: %d >= %d\n", cp.clock, clock)
			return cp.pusher, true
		} else {
			debugf("ignoring cached value to: %d < %d\n", cp.clock, clock)
		}
	}

	if len(values) == 1 {
		if bytes.Equal(values[0].Value, ct.selfId) {
			return nil, false
		}

		id := string(values[0].Value)

		if cp, ok := ct.connections[id]; ok {
			return cp, true
		}

		cp := &consulPusher{nil, string(values[0].Value)}

		ct.connections[id] = cp

		cp.Connect()

		ct.cache[name] = &cachedPusher{clock, cp}

		return cp, true
	}

	mp := NewMultiPusher()

	for _, val := range values {
		id := string(val.Value)

		if bytes.Equal(val.Value, ct.selfId) {
			continue
		}

		if cp, ok := ct.connections[id]; ok {
			mp.Add(cp)
		} else {
			cp := &consulPusher{nil, id}

			ct.connections[id] = cp

			cp.Connect()

			mp.Add(cp)
		}
	}

	if len(mp.pushers) == 1 {
		sp := mp.pushers[0]

		ct.cache[name] = &cachedPusher{clock, sp}

		return sp, true
	}

	ct.cache[name] = &cachedPusher{clock, mp}

	return mp, true
}

func (ct *consulRoutingTable) Cleanup() error {
	for name, _ := range ct.names {
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
	c, err := NewClient(cp.target)
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
