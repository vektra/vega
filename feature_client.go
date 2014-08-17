package vega

import (
	"errors"
	"io"
	"net"
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

type pipeAddr struct {
	q string
}

func (p *pipeAddr) Network() string {
	return "vega"
}

func (p *pipeAddr) String() string {
	return "vega:" + p.q
}

type PipeConn struct {
	fc     *FeatureClient
	pairM  string
	ownM   string
	closed bool
	buffer []byte
	bulk   net.Conn

	readDeadline time.Time
}

func (p *PipeConn) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	p.fc.Abandon(p.ownM)
	p.fc.Push(p.pairM, &Message{Type: "pipe/close"})
	return nil
}

func (p *PipeConn) LocalAddr() net.Addr {
	return &pipeAddr{p.ownM}
}

func (p *PipeConn) RemoteAddr() net.Addr {
	return &pipeAddr{p.pairM}
}

var ETimeout = errors.New("operation timeout")

func (p *PipeConn) Read(b []byte) (int, error) {
	if p.closed {
		return 0, io.EOF
	}

	if p.bulk != nil {
		n, err := p.bulk.Read(b)
		if err == io.EOF {
			p.bulk.Close()
			p.bulk = nil
		} else {
			return n, err
		}
	}

	total := 0
	timeout := 1 * time.Minute

	if p.buffer != nil {
		n := len(p.buffer)
		bn := len(b)

		if bn < n {
			copy(b, p.buffer[:bn])
			p.buffer = p.buffer[bn:]
			return bn, nil
		}

		copy(b, p.buffer)
		p.buffer = nil

		if bn == n {
			return n, nil
		}

		total += n
		timeout = 0 * time.Second
		b = b[n:]
	}

	for {
		var resp *Delivery
		var err error

		if timeout == 0 {
			resp, err = p.fc.Poll(p.ownM)

			if resp == nil {
				if total > 0 {
					return total, nil
				}

				return 0, err
			}
		} else {
			if !p.readDeadline.IsZero() {
				dur := p.readDeadline.Sub(time.Now())
				if dur < timeout {
					timeout = dur
				}
			}

			resp, err = p.fc.LongPoll(p.ownM, timeout)
			if err != nil {
				return 0, err
			}

			if resp == nil {
				if !p.readDeadline.IsZero() && time.Now().After(p.readDeadline) {
					return 0, ETimeout
				}

				continue
			}
		}

		err = resp.Ack()
		if err != nil {
			return 0, err
		}

		switch resp.Message.Type {
		case "pipe/close":
			p.Close()

			if total > 0 {
				return total, nil
			}

			return 0, io.EOF
		case "pipe/bulkstart":
			return p.readBulk(resp.Message, b)
		}

		bn := len(b)
		n := len(resp.Message.Body)

		if bn < n {
			copy(b, resp.Message.Body[:bn])
			p.buffer = resp.Message.Body[bn:]
			return bn + total, nil
		}

		copy(b, resp.Message.Body)
		p.buffer = nil

		total += n

		if bn == n {
			return total, nil
		}

		timeout = 0 * time.Second

		b = b[n:]
	}
}

func (p *PipeConn) Write(b []byte) (int, error) {
	if p.closed {
		return 0, io.EOF
	}

	msg := Message{
		Body: b,
	}

	err := p.fc.Push(p.pairM, &msg)
	if err != nil {
		return 0, err
	}

	return len(b), nil
}

func (p *PipeConn) SetDeadline(t time.Time) error {
	p.readDeadline = t
	return nil
}

func (p *PipeConn) SetReadDeadline(t time.Time) error {
	p.readDeadline = t
	return nil
}

func (p *PipeConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (p *PipeConn) readBulk(msg *Message, data []byte) (int, error) {
	socketId := msg.CorrelationId

	s, err := net.Dial("tcp", socketId)
	if err != nil {
		return 0, err
	}

	p.bulk = s

	return s.Read(data)
}

func (p *PipeConn) SendBulk(data io.Reader) (int64, error) {
	if p.closed {
		return 0, io.EOF
	}

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}

	defer l.Close()

	msg := Message{
		Type:          "pipe/bulkstart",
		CorrelationId: l.Addr().String(),
	}

	err = p.fc.Push(p.pairM, &msg)
	if err != nil {
		return 0, err
	}

	s, err := l.Accept()
	if err != nil {
		return 0, err
	}

	defer s.Close()

	n, err := io.Copy(s, data)
	return n, err
}

func (fc *FeatureClient) ListenPipe(name string) (*PipeConn, error) {
	q := "pipe:" + name
	err := fc.Declare(q)
	if err != nil {
		return nil, err
	}

	for {
		resp, err := fc.LongPoll(q, 1*time.Minute)
		if err != nil {
			return nil, err
		}

		if resp == nil {
			continue
		}

		err = resp.Ack()
		if err != nil {
			return nil, err
		}

		if resp.Message.Type != "pipe/initconnect" {
			return nil, EProtocolError
		}

		ownM := RandomQueue()
		fc.EphemeralDeclare(ownM)

		msg := Message{
			Type:    "pipe/setup",
			ReplyTo: ownM,
		}

		err = fc.Push(resp.Message.ReplyTo, &msg)
		if err != nil {
			fc.Abandon(ownM)
			return nil, err
		}

		return &PipeConn{
			fc:    fc,
			pairM: resp.Message.ReplyTo,
			ownM:  ownM}, nil
	}
}

func (fc *FeatureClient) ConnectPipe(name string) (net.Conn, error) {
	ownM := RandomQueue()
	fc.EphemeralDeclare(ownM)

	msg := Message{
		Type:    "pipe/initconnect",
		ReplyTo: ownM,
	}

	q := "pipe:" + name

	err := fc.Push(q, &msg)
	if err != nil {
		fc.Abandon(ownM)
		return nil, err
	}

	for {
		resp, err := fc.LongPoll(ownM, 1*time.Minute)
		if err != nil {
			return nil, err
		}

		if resp == nil {
			continue
		}

		err = resp.Ack()
		if err != nil {
			return nil, err
		}

		if resp.Message.Type != "pipe/setup" {
			fc.Abandon(ownM)
			return nil, EProtocolError
		}

		return &PipeConn{
			fc:    fc,
			pairM: resp.Message.ReplyTo,
			ownM:  ownM}, nil
	}
}
