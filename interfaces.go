package mailbox

import "time"

type MailboxStats struct {
	Size int
}

type Mailbox interface {
	Push(*Message) error
	Poll() (*Message, error)
	AddWatcher() <-chan *Message
	Stats() *MailboxStats
}

type Storage interface {
	Declare(string) error
	Push(string, *Message) error
	Poll(string) (*Message, error)
	LongPoll(string, time.Duration) (*Message, error)
}

type RouteTable interface {
	Set(string, Storage) error
	Get(string) (Storage, bool)
}
