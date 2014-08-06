package mailbox

import (
	"errors"
	"io"
	"net"
	"time"

	"github.com/ugorji/go/codec"

	messages "./messages"
)

var msgpack codec.MsgpackHandle

var EProtocolError = errors.New("protocol error")

type Storage interface {
	Declare(string) error
	Push(string, []byte) error
	Poll(string) ([]byte, bool)
	LongPoll(string, time.Duration) ([]byte, bool)
}

type Service struct {
	Address  string
	Registry Storage

	listener net.Listener
}

func NewService(addr string) (*Service, error) {
	l, err := net.Listen("tcp", addr)

	if err != nil {
		return nil, err
	}

	return &Service{
		Address:  addr,
		Registry: registry(),
		listener: l,
	}, nil
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

		switch messages.MessageType(buf[0]) {
		case messages.DeclareType:
			msg := &messages.Declare{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				return
			}

			err = s.handleDeclare(c, msg)
		case messages.PollType:
			msg := &messages.Poll{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				panic(err)
			}

			err = s.handlePoll(c, msg)
		case messages.LongPollType:
			msg := &messages.LongPoll{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				panic(err)
			}

			err = s.handleLongPoll(c, msg)
		case messages.PushType:
			msg := &messages.Push{}
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
			c.Write([]byte{uint8(messages.ErrorType)})

			enc := codec.NewEncoder(c, &msgpack)

			err = enc.Encode(&messages.Error{err.Error()})
			if err != nil {
				panic(err)
			}
		}
	}
}

func (s *Service) handleDeclare(c net.Conn, msg *messages.Declare) error {
	err := s.Registry.Declare(msg.Name)
	if err != nil {
		return err
	}

	c.Write([]byte{uint8(messages.SuccessType)})
	return nil
}

func (s *Service) handlePoll(c net.Conn, msg *messages.Poll) error {
	var ret messages.PollResult

	val, ok := s.Registry.Poll(msg.Name)
	if ok {
		ret.Message = &messages.Message{Body: val}
	}

	c.Write([]byte{uint8(messages.PollResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&ret)
}

func (s *Service) handleLongPoll(c net.Conn, msg *messages.LongPoll) error {
	dur, err := time.ParseDuration(msg.Duration)
	if err != nil {
		return err
	}

	var ret messages.PollResult

	val, ok := s.Registry.LongPoll(msg.Name, dur)
	if ok {
		ret.Message = &messages.Message{Body: val}
	}

	c.Write([]byte{uint8(messages.PollResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&ret)
}

func (s *Service) handlePush(c net.Conn, msg *messages.Push) error {
	err := s.Registry.Push(msg.Name, msg.Message.Body)
	if err != nil {
		return err
	}

	c.Write([]byte{uint8(messages.SuccessType)})
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
	c.conn.Write([]byte{uint8(messages.DeclareType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := messages.Declare{
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

	switch messages.MessageType(buf[0]) {
	case messages.ErrorType:
		var msgerr messages.Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return err
		}

		return errors.New(msgerr.Error)
	case messages.SuccessType:
		return nil
	default:
		return EProtocolError
	}
}

func (c *Client) Poll(name string) (*messages.Message, error) {
	c.conn.Write([]byte{uint8(messages.PollType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := messages.Poll{
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

	switch messages.MessageType(buf[0]) {
	case messages.ErrorType:
		var msgerr messages.Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(msgerr.Error)
	case messages.PollResultType:
		dec := codec.NewDecoder(c.conn, &msgpack)

		var res messages.PollResult

		if err := dec.Decode(&res); err != nil {
			return nil, err
		}

		return res.Message, nil
	default:
		return nil, EProtocolError
	}
}

func (c *Client) LongPoll(name string, til time.Duration) (*messages.Message, error) {
	c.conn.Write([]byte{uint8(messages.LongPollType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := messages.LongPoll{
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

	switch messages.MessageType(buf[0]) {
	case messages.ErrorType:
		var msgerr messages.Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return nil, err
		}

		return nil, errors.New(msgerr.Error)
	case messages.PollResultType:
		dec := codec.NewDecoder(c.conn, &msgpack)

		var res messages.PollResult

		if err := dec.Decode(&res); err != nil {
			return nil, err
		}

		return res.Message, nil
	default:
		return nil, EProtocolError
	}
}

func (c *Client) Push(name string, body []byte) error {
	c.conn.Write([]byte{uint8(messages.PushType)})

	enc := codec.NewEncoder(c.conn, &msgpack)

	msg := messages.Push{
		Name:    name,
		Message: &messages.Message{body},
	}

	if err := enc.Encode(&msg); err != nil {
		return err
	}

	buf := []byte{0}

	_, err := io.ReadFull(c.conn, buf)
	if err != nil {
		return err
	}

	switch messages.MessageType(buf[0]) {
	case messages.ErrorType:
		var msgerr messages.Error

		err = codec.NewDecoder(c.conn, &msgpack).Decode(&msgerr)
		if err != nil {
			return err
		}

		return errors.New(msgerr.Error)
	case messages.SuccessType:
		return nil
	default:
		return EProtocolError
	}
}
