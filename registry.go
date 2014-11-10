package vega

import (
	"sync"
	"time"

	"github.com/vektra/errors"
)

type Registry struct {
	sync.Mutex

	mailboxes map[string]Mailbox
	creator   func(string) Mailbox
}

func NewRegistry(create func(string) Mailbox) *Registry {
	return &Registry{
		mailboxes: make(map[string]Mailbox),
		creator:   create,
	}
}

func NewMemRegistry() *Registry {
	return NewRegistry(NewMemMailbox)
}

func (r *Registry) Poll(name string) (*Delivery, error) {
	r.Lock()
	defer r.Unlock()

	if mailbox, ok := r.mailboxes[name]; ok {
		msg, err := mailbox.Poll()
		if err != nil {
			return nil, err
		}

		if msg == nil {
			return nil, nil
		}

		return NewDelivery(mailbox, msg), nil
	}

	return nil, nil
}

func (r *Registry) LongPoll(name string, til time.Duration) (*Delivery, error) {
	r.Lock()

	mailbox, ok := r.mailboxes[name]
	if !ok {
		debugf("missing mailbox %s\n", name)
		r.Unlock()
		return nil, errors.Subject(ENoMailbox, name)
	}

	debugf("long polling %s: %#v\n", name, mailbox)
	val, err := mailbox.Poll()
	if err != nil {
		r.Unlock()
		return nil, err
	}

	if val != nil {
		r.Unlock()
		return NewDelivery(mailbox, val), nil
	}

	indicator := mailbox.AddWatcher()

	r.Unlock()

	select {
	case val := <-indicator:
		if val == nil {
			return nil, nil
		}

		return NewDelivery(mailbox, val), nil
	case <-time.Tick(til):
		return nil, nil
	}
}

func (r *Registry) LongPollCancelable(name string, til time.Duration, done chan struct{}) (*Delivery, error) {
	r.Lock()

	mailbox, ok := r.mailboxes[name]
	if !ok {
		debugf("missing mailbox %s\n", name)
		r.Unlock()
		return nil, errors.Subject(ENoMailbox, name)
	}

	debugf("long polling %s: %#v\n", name, mailbox)
	val, err := mailbox.Poll()
	if err != nil {
		r.Unlock()
		return nil, err
	}

	if val != nil {
		r.Unlock()
		return NewDelivery(mailbox, val), nil
	}

	indicator := mailbox.AddWatcherCancelable(done)

	r.Unlock()

	select {
	case <-done:
		// It's possible for both done and indicator to have values.
		// So we need to also check if there a value in indicator and
		// if so, pull it out and nack it.

		select {
		case val := <-indicator:
			if val != nil {
				mailbox.Nack(val.MessageId)
			}
		default:
		}

		return nil, nil
	case val := <-indicator:
		// It's possible for both done and indicator to have values.
		// So we need to also check if there a value in indicator and
		// if so, pull it out and nack it.

		select {
		case <-done:
			if val != nil {
				mailbox.Nack(val.MessageId)
				return nil, nil
			}
		default:
		}

		if val == nil {
			return nil, nil
		}

		return NewDelivery(mailbox, val), nil
	case <-time.Tick(til):
		return nil, nil
	}
}

var ENoMailbox = errors.New("No such mailbox available")

func (r *Registry) Push(name string, value *Message) error {
	r.Lock()
	defer r.Unlock()

	if mailbox, ok := r.mailboxes[name]; ok {
		return mailbox.Push(value)
	}

	return errors.Subject(ENoMailbox, name)
}

func (r *Registry) Declare(name string) error {
	r.Lock()
	defer r.Unlock()

	debugf("declaring mailbox: '%s'\n", name)

	if _, ok := r.mailboxes[name]; !ok {
		r.mailboxes[name] = r.creator(name)
	}

	return nil
}

func (r *Registry) Abandon(name string) error {
	r.Lock()
	defer r.Unlock()

	if m, ok := r.mailboxes[name]; ok {
		m.Abandon()
		delete(r.mailboxes, name)
	}

	return nil
}
