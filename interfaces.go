package mailbox

import "time"

type MailboxStats struct {
	Size int
}

type Mailbox interface {
	Push(*Message) error
	Poll() (*Message, bool)
	AddWatcher() <-chan *Message
	Stats() *MailboxStats
}

type Storage interface {
	Declare(string) error
	Push(string, *Message) error
	Poll(string) (*Message, bool)
	LongPoll(string, time.Duration) (*Message, bool)
}
