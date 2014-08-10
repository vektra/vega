package mailbox

import (
	"errors"
	"sync"
	"time"
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

func (r *Registry) Poll(name string) (*Message, error) {
	r.Lock()
	defer r.Unlock()

	if mailbox, ok := r.mailboxes[name]; ok {
		return mailbox.Poll()
	}

	return nil, nil
}

func (r *Registry) LongPoll(name string, til time.Duration) (*Message, error) {
	r.Lock()

	mailbox, ok := r.mailboxes[name]
	if !ok {
		debugf("missing mailbox %s\n", name)
		r.Unlock()
		return nil, ENoMailbox
	}

	debugf("long polling %s: %#v\n", name, mailbox)
	val, err := mailbox.Poll()
	if err != nil {
		r.Unlock()
		return nil, err
	}

	if val != nil {
		r.Unlock()
		return val, nil
	}

	indicator := mailbox.AddWatcher()

	r.Unlock()

	select {
	case val := <-indicator:
		if val != nil {
			return val, nil
		} else {
			return val, nil
		}
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

	return ENoMailbox
}

func (r *Registry) Declare(name string) error {
	r.Lock()
	defer r.Unlock()

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
