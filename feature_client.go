package mailbox

import (
	"strings"
	"sync"
	"time"
)

func Dial(addr string) (*FeatureClient, error) {
	client, err := NewClient(addr)
	if err != nil {
		return nil, err
	}

	return &FeatureClient{
		Client: client,
	}, nil
}

// Create a new FeatureClient wrapping a explicit Client
func NewFeatureClient(c *Client) *FeatureClient {
	return &FeatureClient{Client: c}
}

type Handler interface {
	HandleMessage(*Message) *Message
}

type wrappedHandlerFunc struct {
	f func(*Message) *Message
}

func (w *wrappedHandlerFunc) HandleMessage(m *Message) *Message {
	return w.f(m)
}

func HandlerFunc(h func(*Message) *Message) Handler {
	return &wrappedHandlerFunc{h}
}

// type Handler func(*Message) *Message

// Wraps Client to provide highlevel behaviors that build on the basics
// of the distributed mailboxes. Should only be used by one goroutine
// at a time.
type FeatureClient struct {
	*Client

	localQueue string
	lock       sync.Mutex
}

// Create a new FeatureClient that wraps the same Client as
// this one. Useful for creating a new instance to use in a new
// goroutine
func (fc *FeatureClient) Clone() *FeatureClient {
	return &FeatureClient{Client: fc.Client}
}

// Return the name of a ephemeral queue only for this instance
func (fc *FeatureClient) LocalQueue() string {
	fc.lock.Lock()
	defer fc.lock.Unlock()

	if fc.localQueue != "" {
		return fc.localQueue
	}

	r := RandomQueue()

	err := fc.EphemeralDeclare(r)
	if err != nil {
		panic(err)
	}

	fc.localQueue = r

	return r
}

const cEphemeral = "#ephemeral"

func (fc *FeatureClient) Declare(name string) error {
	if strings.HasSuffix(name, cEphemeral) {
		return fc.Client.EphemeralDeclare(name)
	}

	return fc.Client.Declare(name)
}

func (fc *FeatureClient) HandleRequests(name string, h Handler) error {
	for {
		del, err := fc.LongPoll(name, 1*time.Minute)
		if err != nil {
			return err
		}

		if del == nil {
			continue
		}

		msg := del.Message

		ret := h.HandleMessage(msg)

		del.Ack()

		fc.Push(msg.ReplyTo, ret)
	}
}

func (fc *FeatureClient) Request(name string, msg *Message) (*Delivery, error) {
	msg.ReplyTo = fc.LocalQueue()

	err := fc.Push(name, msg)
	if err != nil {
		return nil, err
	}

	for {
		resp, err := fc.LongPoll(msg.ReplyTo, 1*time.Minute)
		if err != nil {
			return nil, err
		}

		if resp == nil {
			continue
		}

		return resp, nil
	}
}

type Receiver struct {
	// channel that messages are sent to
	Channel <-chan *Delivery

	// Any error detected while receiving
	Error error

	shutdown chan struct{}
}

func (rec *Receiver) Close() error {
	close(rec.shutdown)
	return nil
}

func (fc *FeatureClient) Receive(name string) *Receiver {
	c := make(chan *Delivery)

	rec := &Receiver{c, nil, make(chan struct{})}

	go func() {
		for {
			select {
			case <-rec.shutdown:
				close(c)
				return
			default:
				// We don't cancel this action if Receive is told to Close. Instead
				// we let it timeout and then detect the shutdown request and exit.
				msg, err := fc.Client.LongPoll(name, 1*time.Minute)
				if err != nil {
					close(c)
					return
				}

				if msg == nil {
					continue
				}

				c <- msg
			}
		}
	}()

	return rec
}
