package cluster

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	"strings"
	"sync"

	"github.com/armon/consul-api"
	"github.com/vektra/vega"
)

type consulRoutingTable struct {
	selfId []byte
	key    string
	names  map[string]bool

	lock sync.RWMutex

	local vega.RouteTable

	consul *consulapi.Client

	prefix string
	kv     *consulapi.KV
	done   bool

	connections map[string]*consulPusher

	tableLock sync.RWMutex
	table     map[string]*hybridPusher
}

type hybridPusher struct {
	local  vega.Pusher
	remote []*consulPusher
}

func (h *hybridPusher) Push(who string, msg *vega.Message) error {
	if h.local != nil {
		if err := h.local.Push(who, msg); err != nil {
			return err
		}
	}

	for _, r := range h.remote {
		if err := r.Push(who, msg); err != nil {
			return err
		}
	}

	return nil
}

func (h *hybridPusher) Count() int {
	if h.local != nil {
		return len(h.remote) + 1
	} else {
		return len(h.remote)
	}
}

var DefaultRoutingPrefix = "mailbox-routing"

func NewConsulRoutingTable(prefix, id string, client *consulapi.Client) (*consulRoutingTable, error) {
	h := sha1.New()
	h.Write([]byte(id))
	k := hex.EncodeToString(h.Sum(nil))

	ct := &consulRoutingTable{
		selfId:      []byte(id),
		key:         k,
		names:       make(map[string]bool),
		local:       make(vega.MemRouteTable),
		connections: make(map[string]*consulPusher),

		consul: client,
		kv:     client.KV(),

		table: make(map[string]*hybridPusher),
	}

	go ct.updateBackground()

	return ct, nil
}

func (ct *consulRoutingTable) extractName(val *consulapi.KVPair) (string, string) {
	start := len(ct.prefix)

	slashIndex := strings.Index(val.Key[start:], "/")

	return val.Key[start:slashIndex], val.Key[slashIndex+1:]
}

func (ct *consulRoutingTable) updateBackground() {
	qo := &consulapi.QueryOptions{WaitIndex: 0, WaitTime: 1 * time.Minute}

	for {
		if ct.done {
			return
		}

		values, qm, err := ct.kv.List(ct.prefix, qo)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		// We're seeing the same state as we last saw, so skip it.
		if qo.WaitIndex == qm.LastIndex {
			continue
		}

		// fmt.Printf("updating at %d: %d values\n", qo.WaitIndex, len(values))

		qo.WaitIndex = qm.LastIndex

		ct.tableLock.Lock()

		// Clear out the existing remotes because we're going to repopulate
		// them now.
		for _, ent := range ct.table {
			ent.remote = nil
		}

		for _, val := range values {
			name, id := ct.extractName(val)

			if bytes.Equal(val.Value, ct.selfId) {
				continue
			}

			// fmt.Printf(" => %v, %v (%v)\n", name, id, string(val.Value))

			ent, ok := ct.table[name]
			if !ok {
				ent = &hybridPusher{}
				ct.table[name] = ent
			}

			ent.remote = append(ent.remote, ct.connectionTo(id, string(val.Value)))
		}

		var toRemove []string

		for key, ent := range ct.table {
			if ent.local == nil && len(ent.remote) == 0 {
				toRemove = append(toRemove, key)
			}
		}

		for _, key := range toRemove {
			delete(ct.table, key)
		}

		ct.tableLock.Unlock()
	}
}

func (ct *consulRoutingTable) Close() {
	ct.done = true
}

func (ct *consulRoutingTable) Set(name string, p vega.Pusher) error {
	ct.tableLock.Lock()
	defer ct.tableLock.Unlock()

	err := ct.local.Set(name, p)
	if err != nil {
		return err
	}

	ent, ok := ct.table[name]
	if !ok {
		ent = &hybridPusher{}
		ct.table[name] = ent
	}

	lp, ok := ct.local.Get(name)
	if !ok {
		return fmt.Errorf("local registery is corrupted")
	}

	ent.local = lp

	key := ct.prefix + name + "/" + ct.key

	pair := &consulapi.KVPair{
		Key:   key,
		Value: ct.selfId,
	}

	_, err = ct.kv.Put(pair, &consulapi.WriteOptions{})
	if err != nil {
		return err
	}

	ct.names[name] = true
	return nil
}

func (ct *consulRoutingTable) Remove(name string) error {
	delete(ct.names, name)

	key := name + "/" + ct.key

	ct.kv.Delete(key, nil)
	return ct.local.Remove(name)
}

func (ct *consulRoutingTable) connectionTo(id, target string) *consulPusher {
	if cp, ok := ct.connections[id]; ok {
		return cp
	}

	cp := &consulPusher{nil, target}

	ct.connections[id] = cp

	cp.Connect()

	return cp
}

type consulPusher struct {
	client vega.Storage
	target string
}

func (ct *consulRoutingTable) Get(name string) (vega.Pusher, bool) {
	ct.tableLock.RLock()

	pusher, ok := ct.table[name]

	ct.tableLock.RUnlock()

	return pusher, ok
}

func (ct *consulRoutingTable) Cleanup() error {
	for name, _ := range ct.names {
		key := ct.prefix + name + "/" + ct.key
		ct.kv.Delete(key, nil)
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
