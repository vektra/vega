package mailbox

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ugorji/go/codec"
)

var msgpack codec.MsgpackHandle

var EProtocolError = errors.New("protocol error")

type Service struct {
	Address  string
	Registry Storage

	listener net.Listener

	wg     sync.WaitGroup
	closed bool
}

func NewService(addr string, reg Storage) (*Service, error) {
	l, err := net.Listen("tcp", addr)

	if err != nil {
		return nil, err
	}

	s := &Service{
		Address:  addr,
		Registry: reg,
		listener: l,
	}

	s.wg.Add(1)

	go s.Accept()

	return s, nil
}

func NewMemService(addr string) (*Service, error) {
	return NewService(addr, NewMemRegistry())
}

func (s *Service) Close() error {
	s.closed = true
	s.listener.Close()
	s.wg.Wait()
	return nil
}

func (s *Service) Accept() error {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		go s.handle(conn)
	}

	return nil
}

func (s *Service) handle(c net.Conn) {
	buf := []byte{0}

	for {
		n, err := c.Read(buf)
		if n == 0 || err != nil {
			return
		}

		switch MessageType(buf[0]) {
		case DeclareType:
			msg := &Declare{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				return
			}

			err = s.handleDeclare(c, msg)
		case PollType:
			msg := &Poll{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				panic(err)
			}

			err = s.handlePoll(c, msg)
		case LongPollType:
			msg := &LongPoll{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				panic(err)
			}

			err = s.handleLongPoll(c, msg)
		case PushType:
			msg := &Push{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				return
			}

			err = s.handlePush(c, msg)
		default:
			err = EProtocolError
		}

		if err != nil {
			c.Write([]byte{uint8(ErrorType)})

			enc := codec.NewEncoder(c, &msgpack)

			err = enc.Encode(&Error{err.Error()})
			if err != nil {
				panic(err)
			}
		}
	}
}

func (s *Service) handleDeclare(c net.Conn, msg *Declare) error {
	err := s.Registry.Declare(msg.Name)
	if err != nil {
		return err
	}

	_, err = c.Write([]byte{uint8(SuccessType)})
	return err
}

func (s *Service) handlePoll(c net.Conn, msg *Poll) error {
	var ret PollResult

	val, err := s.Registry.Poll(msg.Name)
	if err != nil {
		return err
	}

	ret.Message = val

	c.Write([]byte{uint8(PollResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&ret)
}

func (s *Service) handleLongPoll(c net.Conn, msg *LongPoll) error {
	debugf("handleLongPoll for %#v\n", s.Registry)

	dur, err := time.ParseDuration(msg.Duration)
	if err != nil {
		return err
	}

	var ret PollResult

	val, err := s.Registry.LongPoll(msg.Name, dur)
	if err != nil {
		return err
	}

	ret.Message = val

	c.Write([]byte{uint8(PollResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&ret)
}

func (s *Service) handlePush(c net.Conn, msg *Push) error {
	debugf("%s: handlePush for %#v\n", s.Address, s.Registry)

	err := s.Registry.Push(msg.Name, msg.Message)
	if err != nil {
		return err
	}

	debugf("%s: sending success\n", s.Address)

	_, err = c.Write([]byte{uint8(SuccessType)})
	return err
}

type Client struct {
	conn net.Conn
	addr string
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn, addr}, nil
}

func (c *Client) checkError(err error) error {
	debugf("client %s error: %s\n", c.addr, err)
	if err == io.EOF {
		c.conn = nil
	}

	return err
}

func (c *Client) Client() (net.Conn, error) {
	if c.conn == nil {
		s, err := net.Dial("tcp", c.addr)
		if err != nil {
			return nil, err
		}

		c.conn = s
	}

	return c.conn, nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) Declare(name string) error {
	s, err := c.Client()
	if err != nil {
		return err
	}

	_, err = s.Write([]byte{uint8(DeclareType)})
	if err != nil {
		return c.checkError(err)
	}

	enc := codec.NewEncoder(s, &msgpack)

	msg := Declare{
		Name: name,
	}

	err = enc.Encode(&msg)
	if err != nil {
		return c.checkError(err)
	}

	buf := []byte{0}

	_, err = io.ReadFull(s, buf)
	if err != nil {
		return c.checkError(err)
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(s, &msgpack).Decode(&msgerr)
		if err != nil {
			return c.checkError(err)
		}

		return errors.New(msgerr.Error)
	case SuccessType:
		return nil
	default:
		return c.checkError(EProtocolError)
	}
}

func (c *Client) Poll(name string) (*Message, error) {
	s, err := c.Client()
	if err != nil {
		return nil, err
	}

	_, err = s.Write([]byte{uint8(PollType)})
	if err != nil {
		return nil, c.checkError(err)
	}

	enc := codec.NewEncoder(s, &msgpack)

	msg := Poll{
		Name: name,
	}

	if err := enc.Encode(&msg); err != nil {
		return nil, c.checkError(err)
	}

	buf := []byte{0}

	_, err = io.ReadFull(s, buf)
	if err != nil {
		return nil, c.checkError(err)
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(s, &msgpack).Decode(&msgerr)
		if err != nil {
			return nil, c.checkError(err)
		}

		return nil, errors.New(msgerr.Error)
	case PollResultType:
		dec := codec.NewDecoder(s, &msgpack)

		var res PollResult

		if err := dec.Decode(&res); err != nil {
			return nil, c.checkError(err)
		}

		return res.Message, nil
	default:
		return nil, c.checkError(EProtocolError)
	}
}

func (c *Client) LongPoll(name string, til time.Duration) (*Message, error) {
	s, err := c.Client()
	if err != nil {
		return nil, err
	}

	_, err = s.Write([]byte{uint8(LongPollType)})
	if err != nil {
		return nil, c.checkError(err)
	}

	enc := codec.NewEncoder(s, &msgpack)

	msg := LongPoll{
		Name:     name,
		Duration: til.String(),
	}

	if err := enc.Encode(&msg); err != nil {
		return nil, c.checkError(err)
	}

	buf := []byte{0}

	_, err = io.ReadFull(s, buf)
	if err != nil {
		return nil, c.checkError(err)
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(s, &msgpack).Decode(&msgerr)
		if err != nil {
			return nil, c.checkError(err)
		}

		return nil, errors.New(msgerr.Error)
	case PollResultType:
		dec := codec.NewDecoder(s, &msgpack)

		var res PollResult

		if err := dec.Decode(&res); err != nil {
			return nil, c.checkError(err)
		}

		return res.Message, nil
	default:
		return nil, c.checkError(EProtocolError)
	}
}

func (c *Client) Push(name string, body *Message) error {
	s, err := c.Client()
	if err != nil {
		return err
	}

	_, err = s.Write([]byte{uint8(PushType)})
	if err != nil {
		return c.checkError(err)
	}

	enc := codec.NewEncoder(s, &msgpack)

	msg := Push{
		Name:    name,
		Message: body,
	}

	debugf("client %s: sending push request\n", c.addr)

	if err := enc.Encode(&msg); err != nil {
		return c.checkError(err)
	}

	buf := []byte{0}

	debugf("client %s: waiting for response\n", c.addr)

	_, err = io.ReadFull(s, buf)
	if err != nil {
		return c.checkError(err)
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		debugf("client %s: got error\n", c.addr)

		var msgerr Error

		err = codec.NewDecoder(s, &msgpack).Decode(&msgerr)
		if err != nil {
			return c.checkError(err)
		}

		return errors.New(msgerr.Error)
	case SuccessType:
		debugf("client %s: got success\n", c.addr)

		return nil
	default:
		debugf("client %s: got protocol error\n", c.addr)

		return c.checkError(EProtocolError)
	}
}
