package mailbox

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
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

	lock sync.Mutex
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

		go s.acceptMux(conn)
	}

	return nil
}

func eofish(err error) bool {
	if err == io.EOF {
		return true
	}

	if err == yamux.ErrSessionShutdown {
		return true
	}

	if strings.Index(err.Error(), "connection reset by peer") != -1 {
		return true
	}

	if strings.Index(err.Error(), "broken pipe") != -1 {
		return true
	}

	return false
}

func (s *Service) cleanupConn(c net.Conn, data *clientData) {
	s.lock.Lock()

	if data.inflight != nil {
		for _, del := range data.inflight {
			del.Nack()
		}

		data.inflight = nil
	}

	for _, name := range data.ephemerals {
		s.Registry.Abandon(name)
	}

	data.ephemerals = nil

	data.session.Close()

	s.lock.Unlock()
}

type clientData struct {
	session    *yamux.Session
	inflight   map[MessageId]*Delivery
	ephemerals []string
}

func (s *Service) acceptMux(c net.Conn) {
	session, err := yamux.Server(c, nil)
	if err != nil {
		if eofish(err) {
			return
		}

		panic(err)
	}

	defer session.Close()

	data := &clientData{session, make(map[MessageId]*Delivery), nil}

	for {
		stream, err := session.Accept()
		if err != nil {
			if eofish(err) {
				debugf("eof detected starting a new stream\n")
				s.cleanupConn(c, data)
				return
			}

			panic(err)
		}

		go s.handle(c, stream, data)
	}
}

func (s *Service) handle(parent, c net.Conn, data *clientData) {
	defer c.Close()

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
		case EphemeralDeclareType:
			msg := &Declare{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				return
			}

			err = s.handleEphemeralDeclare(c, msg, parent, data)
		case AbandonType:
			msg := &Abandon{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				return
			}

			err = s.handleAbandon(c, msg)
		case PollType:
			msg := &Poll{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				panic(err)
			}

			err = s.handlePoll(c, msg, data)
		case LongPollType:
			msg := &LongPoll{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				if eofish(err) {
					return
				}

				panic(err)
			}

			err = s.handleLongPoll(c, msg, data)
		case PushType:
			msg := &Push{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				if eofish(err) {
					return
				}

				panic(err)
			}

			err = s.handlePush(c, msg)
		case CloseType:
			err = s.handleClose(c, parent, data)
		case StatsType:
			err = s.handleStats(c, data)

		case AckType:
			msg := &AckMessage{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				return
			}

			err = s.handleAck(c, msg, data)
		case NackType:
			msg := &NackMessage{}
			dec := codec.NewDecoder(c, &msgpack)

			err = dec.Decode(msg)
			if err != nil {
				return
			}

			err = s.handleNack(c, msg, data)
		default:
			err = EProtocolError
		}

		if err != nil {
			c.Write([]byte{uint8(ErrorType)})

			enc := codec.NewEncoder(c, &msgpack)

			err = enc.Encode(&Error{err.Error()})
			if err != nil {
				switch err {
				case yamux.ErrStreamClosed:
					return
				case yamux.ErrSessionShutdown:
					return
				default:
					panic(err)
				}
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

func (s *Service) handleEphemeralDeclare(
	c net.Conn, msg *Declare,
	parent net.Conn, data *clientData) error {

	err := s.Registry.Declare(msg.Name)
	if err != nil {
		return err
	}

	s.lock.Lock()

	data.ephemerals = append(data.ephemerals, msg.Name)

	s.lock.Unlock()

	_, err = c.Write([]byte{uint8(SuccessType)})
	return err
}

func (s *Service) handleAbandon(c net.Conn, msg *Abandon) error {
	err := s.Registry.Abandon(msg.Name)
	if err != nil {
		return err
	}

	_, err = c.Write([]byte{uint8(SuccessType)})
	return err
}

func (s *Service) handlePoll(c net.Conn, msg *Poll, data *clientData) error {
	var ret PollResult

	val, err := s.Registry.Poll(msg.Name)
	if err != nil {
		return err
	}

	if val != nil {
		data.inflight[val.Message.MessageId] = val
		ret.Message = val.Message
	}

	c.Write([]byte{uint8(PollResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&ret)
}

func (s *Service) handleLongPoll(c net.Conn, msg *LongPoll, data *clientData) error {
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

	if val != nil {
		data.inflight[val.Message.MessageId] = val
		ret.Message = val.Message
	}

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

func (s *Service) handleClose(c, parent net.Conn, data *clientData) error {
	s.cleanupConn(parent, data)

	_, err := c.Write([]byte{uint8(SuccessType)})
	return err
}

func (s *Service) handleStats(c net.Conn, data *clientData) error {
	stats := &ClientStats{
		InFlight: len(data.inflight),
	}

	c.Write([]byte{uint8(StatsResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&stats)
}

func (s *Service) handleAck(c net.Conn, msg *AckMessage, data *clientData) error {
	if del, ok := data.inflight[msg.MessageId]; ok {
		err := del.Ack()
		if err != nil {
			debugf("internal nack error: %s\n", err)
			return err
		}

		debugf("removing %s from inflight\n", msg.MessageId)
		delete(data.inflight, msg.MessageId)
		debugf("inflight now: %#v\n", data.inflight)
	} else {
		return EUnknownMessage
	}

	_, err := c.Write([]byte{uint8(SuccessType)})
	return err
}

func (s *Service) handleNack(c net.Conn, msg *NackMessage, data *clientData) error {
	if del, ok := data.inflight[msg.MessageId]; ok {
		err := del.Nack()
		if err != nil {
			debugf("internal nack error: %s\n", err)
			return err
		}

		debugf("removing %s from inflight\n", msg.MessageId)
		delete(data.inflight, msg.MessageId)
		debugf("inflight now: %#v\n", data.inflight)
	} else {
		return EUnknownMessage
	}

	_, err := c.Write([]byte{uint8(SuccessType)})
	return err
}

type Client struct {
	conn net.Conn
	sess *yamux.Session
	addr string
}

func NewClient(addr string) (*Client, error) {
	cl := &Client{nil, nil, addr}

	cl.Session()

	return cl, nil
}

func (c *Client) checkError(err error) error {
	debugf("client %s error: %s\n", c.addr, err)
	if err == io.EOF {
		c.conn = nil
	}

	return err
}

func (c *Client) Session() (*yamux.Session, error) {
	if c.sess == nil {
		s, err := net.Dial("tcp", c.addr)
		if err != nil {
			return nil, err
		}

		c.conn = s

		sess, err := yamux.Client(c.conn, nil)
		if err != nil {
			return nil, err
		}

		c.sess = sess
	}

	return c.sess, nil
}

func (c *Client) Close() (err error) {
	if c.conn == nil {
		return nil
	}

	sess, err := c.Session()
	if err != nil {
		return err
	}

	s, err := sess.Open()
	if err != nil {
		return err
	}

	_, err = s.Write([]byte{uint8(CloseType)})

	defer func() {
		s.Close()

		err = c.sess.Close()
		c.sess = nil
		c.conn = nil
	}()

	buf := []byte{0}

	io.ReadFull(s, buf)
	if err == nil {
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

	return err
}

func (c *Client) Stats() (*ClientStats, error) {
	if c.conn == nil {
		return nil, nil
	}

	sess, err := c.Session()
	if err != nil {
		return nil, err
	}

	s, err := sess.Open()
	if err != nil {
		return nil, err
	}

	_, err = s.Write([]byte{uint8(StatsType)})

	buf := []byte{0}

	_, err = io.ReadFull(s, buf)
	if err != nil {
		return nil, err
	}

	switch MessageType(buf[0]) {
	case StatsResultType:
		dec := codec.NewDecoder(s, &msgpack)

		var res ClientStats

		if err := dec.Decode(&res); err != nil {
			return nil, c.checkError(err)
		}

		return &res, nil
	case ErrorType:
		var msgerr Error

		err = codec.NewDecoder(s, &msgpack).Decode(&msgerr)
		if err != nil {
			return nil, c.checkError(err)
		}

		return nil, errors.New(msgerr.Error)
	case SuccessType:
		return nil, nil
	default:
		return nil, c.checkError(EProtocolError)
	}
}

func (c *Client) Declare(name string) error {
	sess, err := c.Session()
	if err != nil {
		return err
	}

	s, err := sess.Open()
	if err != nil {
		return err
	}

	defer s.Close()

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

func (c *Client) EphemeralDeclare(name string) error {
	sess, err := c.Session()
	if err != nil {
		return err
	}

	s, err := sess.Open()
	if err != nil {
		return err
	}

	defer s.Close()

	_, err = s.Write([]byte{uint8(EphemeralDeclareType)})
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

func (c *Client) Abandon(name string) error {
	sess, err := c.Session()
	if err != nil {
		return err
	}

	s, err := sess.Open()
	if err != nil {
		return err
	}

	defer s.Close()

	_, err = s.Write([]byte{uint8(AbandonType)})
	if err != nil {
		return c.checkError(err)
	}

	enc := codec.NewEncoder(s, &msgpack)

	msg := Abandon{
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

func (c *Client) ack(id MessageId) error {
	sess, err := c.Session()
	if err != nil {
		return err
	}

	s, err := sess.Open()
	if err != nil {
		return err
	}

	defer s.Close()

	_, err = s.Write([]byte{uint8(AckType)})
	if err != nil {
		return c.checkError(err)
	}

	enc := codec.NewEncoder(s, &msgpack)

	msg := AckMessage{
		MessageId: id,
	}

	if err := enc.Encode(&msg); err != nil {
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

func (c *Client) nack(id MessageId) error {
	sess, err := c.Session()
	if err != nil {
		return err
	}

	s, err := sess.Open()
	if err != nil {
		return err
	}

	defer s.Close()

	_, err = s.Write([]byte{uint8(NackType)})
	if err != nil {
		return c.checkError(err)
	}

	enc := codec.NewEncoder(s, &msgpack)

	msg := NackMessage{
		MessageId: id,
	}

	if err := enc.Encode(&msg); err != nil {
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

func (c *Client) Poll(name string) (*Delivery, error) {
	sess, err := c.Session()
	if err != nil {
		return nil, err
	}

	s, err := sess.Open()
	if err != nil {
		return nil, err
	}

	defer s.Close()

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

		if res.Message == nil {
			return nil, nil
		}

		del := &Delivery{
			Message: res.Message,
			Ack:     func() error { return c.ack(res.Message.MessageId) },
			Nack:    func() error { return c.nack(res.Message.MessageId) },
		}

		return del, nil
	default:
		return nil, c.checkError(EProtocolError)
	}
}

func (c *Client) LongPoll(name string, til time.Duration) (*Delivery, error) {
	sess, err := c.Session()
	if err != nil {
		return nil, err
	}

	s, err := sess.Open()
	if err != nil {
		return nil, err
	}

	defer s.Close()

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

		if res.Message == nil {
			return nil, nil
		}

		del := &Delivery{
			Message: res.Message,
			Ack:     func() error { return c.ack(res.Message.MessageId) },
			Nack:    func() error { return c.nack(res.Message.MessageId) },
		}

		return del, nil
	default:
		return nil, c.checkError(EProtocolError)
	}
}

func (c *Client) Push(name string, body *Message) error {
	sess, err := c.Session()
	if err != nil {
		return err
	}

	s, err := sess.Open()
	if err != nil {
		return err
	}

	defer s.Close()

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
