package mailbox

import "time"

type MailboxStats struct {
	Size int
}

type Mailbox interface {
	Abandon() error
	Push(*Message) error
	Poll() (*Message, error)
	AddWatcher() <-chan *Message
	Stats() *MailboxStats
}

type Storage interface {
	Declare(string) error
	Abandon(string) error
	Push(string, *Message) error
	Poll(string) (*Message, error)
	LongPoll(string, time.Duration) (*Message, error)
}

type Pusher interface {
	Push(string, *Message) error
}

type RouteTable interface {
	Set(string, Pusher) error
	Remove(string) error
	Get(string) (Pusher, bool)
}
