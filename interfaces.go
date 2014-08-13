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
	AddWatcherCancelable(chan struct{}) <-chan *Message
	Stats() *MailboxStats
}

type Acker func() error
type Nacker func() error

type Delivery struct {
	Message *Message
	Ack     Acker
	Nack    Nacker
}

func NewDelivery(m Mailbox, msg *Message) *Delivery {
	return &Delivery{
		Message: msg,
		Ack:     func() error { return m.Ack(msg.MessageId) },
		Nack:    func() error { return m.Nack(msg.MessageId) },
	}
}

type Storage interface {
	Declare(string) error
	Abandon(string) error
	Push(string, *Message) error
	Poll(string) (*Delivery, error)
	LongPoll(string, time.Duration) (*Delivery, error)
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
