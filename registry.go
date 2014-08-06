package mailbox

import (
	"errors"
	"sync"
	"time"
)

type Registry struct {
	sync.Mutex

	mailboxes map[string]Mailbox
}

func registry() *Registry {
	return &Registry{
		mailboxes: make(map[string]Mailbox),
	}
}

func (r *Registry) Poll(name string) (*Message, bool) {
	r.Lock()
	defer r.Unlock()

	if mailbox, ok := r.mailboxes[name]; ok {
		return mailbox.Poll()
	}

	return nil, false
}

func (r *Registry) LongPoll(name string, til time.Duration) (*Message, bool) {
	r.Lock()

	mailbox, ok := r.mailboxes[name]
	if !ok {
		r.Unlock()
		return nil, false
	}

	if val, ok := mailbox.Poll(); ok {
		r.Unlock()
		return val, true
	}

	indicator := mailbox.AddWatcher()

	r.Unlock()

	select {
	case val := <-indicator:
		if val != nil {
			return val, true
		} else {
			return val, false
		}
	case <-time.Tick(til):
		return nil, false
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
		r.mailboxes[name] = NewMemMailbox()
	}

	return nil
}
