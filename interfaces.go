package mailbox

import "errors"
import "time"

type MailboxStats struct {
	Size     int
	InFlight int
}

var EUnknownMessage = errors.New("Unknown message id")

type MessageId string

type Mailbox interface {
	Abandon() error
	Push(*Message) error
	Poll() (*Message, error)
	Ack(MessageId) error
	Nack(MessageId) error
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

type Byter interface {
	Bytes() []byte
}
