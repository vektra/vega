package mailbox

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/ugorji/go/codec"
)

var msgpack codec.MsgpackHandle

var EProtocolError = errors.New("protocol error")

type Service struct {
	Address  string
	Registry Storage

	listener net.Listener
}

func NewService(addr string, reg Storage) (*Service, error) {
	l, err := net.Listen("tcp", addr)

	if err != nil {
		return nil, err
	}

	return &Service{
		Address:  addr,
		Registry: reg,
		listener: l,
	}, nil
}

func NewMemService(addr string) (*Service, error) {
	return NewService(addr, NewMemRegistry())
}

func (s *Service) Close() error {
	s.listener.Close()
	return nil
}

func (s *Service) Accept() error {
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
			panic("unknown message type")
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

	c.Write([]byte{uint8(SuccessType)})
	return nil
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
	debugf("handlePush for %#v\n", msg)

	err := s.Registry.Push(msg.Name, msg.Message)
	if err != nil {
		return err
	}

	c.Write([]byte{uint8(SuccessType)})
	return nil
}

type Client struct {
	conn net.Conn
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Declare(name string) error {
	c.conn.Write([]byte{uint8(DeclareType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := Declare{
		Name: name,
	}

	err := enc.Encode(&msg)
	if err != nil {
		return err
	}

	buf := []byte{0}

	_, err = io.ReadFull(c.conn, buf)
	if err != nil {
		return err
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return err
		}

		return errors.New(msgerr.Error)
	case SuccessType:
		return nil
	default:
		return EProtocolError
	}
}

func (c *Client) Poll(name string) (*Message, error) {
	c.conn.Write([]byte{uint8(PollType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := Poll{
		Name: name,
	}

	if err := enc.Encode(&msg); err != nil {
		return nil, err
	}

	buf := []byte{0}

	_, err := io.ReadFull(c.conn, buf)
	if err != nil {
		return nil, err
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(msgerr.Error)
	case PollResultType:
		dec := codec.NewDecoder(c.conn, &msgpack)

		var res PollResult

		if err := dec.Decode(&res); err != nil {
			return nil, err
		}

		return res.Message, nil
	default:
		return nil, EProtocolError
	}
}

func (c *Client) LongPoll(name string, til time.Duration) (*Message, error) {
	c.conn.Write([]byte{uint8(LongPollType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := LongPoll{
		Name:     name,
		Duration: til.String(),
	}

	if err := enc.Encode(&msg); err != nil {
		return nil, err
	}

	buf := []byte{0}

	_, err := io.ReadFull(c.conn, buf)
	if err != nil {
		return nil, err
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(msgerr.Error)
	case PollResultType:
		dec := codec.NewDecoder(c.conn, &msgpack)

		var res PollResult

		if err := dec.Decode(&res); err != nil {
			return nil, err
		}

		return res.Message, nil
	default:
		return nil, EProtocolError
	}
}

func (c *Client) Push(name string, body *Message) error {
	c.conn.Write([]byte{uint8(PushType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := Push{
		Name:    name,
		Message: body,
	}

	if err := enc.Encode(&msg); err != nil {
		return err
	}

	buf := []byte{0}

	_, err := io.ReadFull(c.conn, buf)
	if err != nil {
		return err
	}

	switch MessageType(buf[0]) {
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return err
		}

		return errors.New(msgerr.Error)
	case SuccessType:
		return nil
	default:
		return EProtocolError
	}
}
