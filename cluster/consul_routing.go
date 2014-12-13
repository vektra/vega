package cluster

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"

	"sync"

	"github.com/armon/consul-api"
	"github.com/vektra/consul_kv_cache/cache"
	"github.com/vektra/vega"
)

type cachedPusher struct {
	clock  cache.ClockValue
	pusher vega.Pusher
	nodes  int
}

type consulRoutingTable struct {
	selfId []byte
	key    string
	names  map[string]bool

	lock sync.RWMutex

	local vega.RouteTable

	consul *cache.ConsulKVCache

	connections map[string]*consulPusher

	cache map[string]*cachedPusher
}

var DefaultRoutingPrefix = "mailbox-routing"

func NewConsulRoutingTable(prefix, id string, client *consulapi.Client) (*consulRoutingTable, error) {
	h := sha1.New()
	h.Write([]byte(id))
	k := hex.EncodeToString(h.Sum(nil))

	consul := cache.NewCustomConsulKVCache(prefix, client)

	go consul.BackgroundUpdate()

	ct := &consulRoutingTable{
		selfId:      []byte(id),
		key:         k,
		names:       make(map[string]bool),
		local:       make(vega.MemRouteTable),
		consul:      consul,
		connections: make(map[string]*consulPusher),
		cache:       make(map[string]*cachedPusher),
	}

	return ct, nil
}

func (ct *consulRoutingTable) Close() {
	ct.consul.Close()
}

func (ct *consulRoutingTable) Set(name string, p vega.Pusher) error {
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
	client vega.Storage
	target string
}

type consulValue struct {
	CreateIndex int
	ModifyIndex int
	Key         string
	Flags       int
	Value       []byte
}

func (ct *consulRoutingTable) Get(name string) (vega.Pusher, bool) {
	if lp, ok := ct.local.Get(name); ok {
		// debugf("found local pusher for %s: %#v\n", name, lp)
		return lp, true
	}

	values, clock := ct.consul.GetPrefix(name)

	if len(values) == 0 {
		return nil, false
	}

	if cp, ok := ct.cache[name]; ok {
		if cp.clock >= clock {
			// debugf("using cached value to: %d >= %d\n", cp.clock, clock)
			if cp.nodes != len(values) {
				// debugf("ignoring cached multipusher, # values don't matcher")
			} else {
				return cp.pusher, true
			}
		} else {
			// debugf("ignoring cached value to: %d < %d\n", cp.clock, clock)
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

		ct.cache[name] = &cachedPusher{clock, cp, 1}

		return cp, true
	}

	mp := vega.NewMultiPusher()

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

	if len(mp.Pushers) == 1 {
		sp := mp.Pushers[0]

		ct.cache[name] = &cachedPusher{clock, sp, 1}

		return sp, true
	}

	ct.cache[name] = &cachedPusher{clock, mp, len(mp.Pushers)}

	return mp, true
}

func (ct *consulRoutingTable) Cleanup() error {
	for name, _ := range ct.names {
		key := name + "/" + ct.key
		ct.consul.Delete(key)
	}

	return nil
}

func (cp *consulPusher) Connect() error {
	c, err := vega.NewClient(cp.target)
	if err != nil {
		return err
	}

	cp.client = vega.NewReliableStorage(c)

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

func (cp *consulPusher) Push(name string, msg *vega.Message) error {
	if cp.client == nil {
		err := cp.Connect()
		if err != nil {
			return err
		}
	}

	return cp.client.Push(name, msg)
}

func (cp *consulPusher) Poll(name string) (*vega.Delivery, error) {
	if cp.client == nil {
		err := cp.Connect()
		if err != nil {
			return nil, err
		}
	}

	return cp.client.Poll(name)
}
