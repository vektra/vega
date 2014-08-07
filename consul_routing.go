package mailbox

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type consulRoutingTable struct {
	selfId []byte
	key    string
	names  []string

	lock sync.RWMutex

	cache map[string]*consulPusher
}

func NewConsulRoutingTable(id string) (*consulRoutingTable, error) {
	h := sha1.New()
	h.Write([]byte(id))
	k := hex.EncodeToString(h.Sum(nil))

	ct := &consulRoutingTable{
		selfId: []byte(id),
		key:    k,
		names:  nil,
		cache:  make(map[string]*consulPusher),
	}

	idx, err := ct.primeCache()
	if err != nil {
		return nil, err
	}

	go ct.updateCache(idx)

	return ct, nil
}

var bTrue = []byte("true\n")

func (ct *consulRoutingTable) Set(name string, p Pusher) {
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
}

type consulPusher struct {
	client *Client
	target string
}

type consulValue struct {
	CreateIndex int
	ModifyIndex int
	Key         string
	Flags       int
	Value       []byte
}

func (ct *consulRoutingTable) ManualGet(name string) (Pusher, bool) {
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

	return &consulPusher{nil, string(values[0].Value)}, true
}

func (ct *consulRoutingTable) Get(name string) (Pusher, bool) {
	ct.lock.RLock()

	cp, ok := ct.cache[name]

	ct.lock.RUnlock()

	return cp, ok
}

func (ct *consulRoutingTable) addEntry(val *consulValue) {
	cp := &consulPusher{nil, string(val.Value)}

	start := strings.Index(val.Key, "mailbox-routing/") + len("mailbox-routing/")

	end := strings.LastIndex(val.Key, "/")

	name := val.Key[start:end]

	debugf("added to the consul route cache: %s => %s\n", name, string(val.Value))

	cp.Connect()

	ct.cache[name] = cp
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

	ct.lock.Lock()

	for _, val := range values {
		ct.addEntry(&val)
	}

	ct.lock.Unlock()

	idx, _ := strconv.Atoi(resp.Header.Get("X-Consul-Index"))

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

		ct.lock.Lock()

		for _, val := range values {
			ct.addEntry(&val)
		}

		ct.lock.Unlock()

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

	cp.client = c
	return nil
}

func (cp *consulPusher) Declare(name string) error {
	return cp.client.Declare(name)
}

func (cp *consulPusher) Push(name string, msg *Message) error {
	return cp.client.Push(name, msg)
}

func (cp *consulPusher) Poll(name string) (*Message, error) {
	return cp.client.Poll(name)
}
