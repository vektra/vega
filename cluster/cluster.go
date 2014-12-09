package cluster

import (
	"sync"
	"time"

	"github.com/vektra/errors"
	"github.com/vektra/vega"
	"github.com/vektra/vega/disk"
)

type clusterNode struct {
	lock   sync.Mutex
	router *vega.Router
	local  *vega.Registry
	disk   *disk.Storage

	setupSubscriber bool
	subscriptions   []*vega.Subscription
}

func NewClusterNode(path string, router *vega.Router) (*clusterNode, error) {
	d, err := disk.NewDiskStorage(path)
	if err != nil {
		return nil, err
	}

	return &clusterNode{
		disk:   d,
		local:  vega.NewRegistry(d.Mailbox),
		router: router,
	}, nil
}

func NewMemClusterNode(path string) (*clusterNode, error) {
	return NewClusterNode(path, vega.MemRouter())
}

func (cn *clusterNode) Registry() *vega.Registry {
	return cn.local
}

func (cn *clusterNode) AddRoute(name string, s vega.Storage) {
	cn.router.Add(name, s)
}

func (cn *clusterNode) Close() error {
	return cn.disk.Close()
}

func (cn *clusterNode) Declare(name string) error {
	cn.local.Declare(name)
	cn.router.Add(name, cn.local)
	return nil
}

func (cn *clusterNode) Abandon(name string) error {
	cn.local.Abandon(name)
	return cn.router.Remove(name)
}

type publishedPusher struct {
	*clusterNode
}

func (pp *publishedPusher) Push(name string, msg *vega.Message) error {
	// debugf("remote publish received!\n")
	return pp.publishLocally(msg)
}

func (cn *clusterNode) subscribe(msg *vega.Message) error {
	cn.lock.Lock()
	defer cn.lock.Unlock()

	// debugf("doing subscribe...\n")

	if !cn.setupSubscriber {
		cn.router.Add(":publish", &publishedPusher{cn})
		cn.setupSubscriber = true
	}

	sub := vega.ParseSubscription(msg.CorrelationId)
	sub.Mailbox = msg.ReplyTo

	cn.subscriptions = append(cn.subscriptions, sub)

	return nil
}

func (cn *clusterNode) publishLocally(msg *vega.Message) error {
	cn.lock.Lock()
	defer cn.lock.Unlock()

	for _, sub := range cn.subscriptions {
		if sub.Match(msg.CorrelationId) {
			cn.router.Push(sub.Mailbox, msg)
		}
	}

	return nil
}

func (cn *clusterNode) publish(msg *vega.Message) error {
	// debugf("performing publish\n")
	err := cn.publishLocally(msg)
	if err != nil {
		return errors.Context(err, "publishLocally")
	}

	return cn.router.Push(":publish", msg)
}

func (cn *clusterNode) Push(name string, msg *vega.Message) error {
	switch name {
	case ":subscribe":
		return cn.subscribe(msg)
	case ":publish":
		return cn.publish(msg)
	default:
		return cn.router.Push(name, msg)
	}
}

func (cn *clusterNode) Poll(name string) (*vega.Delivery, error) {
	return cn.local.Poll(name)
}

func (cn *clusterNode) LongPoll(name string, til time.Duration) (*vega.Delivery, error) {
	return cn.local.LongPoll(name, til)
}

func (cn *clusterNode) LongPollCancelable(name string, til time.Duration, done chan struct{}) (*vega.Delivery, error) {
	return cn.local.LongPollCancelable(name, til, done)
}
