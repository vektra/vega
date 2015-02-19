package vega

import (
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/ugorji/go/codec"
	"github.com/vektra/errors"
	"github.com/vektra/seconn"
)

const DefaultPort = 8475

var msgpack codec.MsgpackHandle

var EProtocolError = errors.New("protocol error")

var muxConfig = yamux.DefaultConfig()

type Service struct {
	Address  string
	Registry Storage

	listener net.Listener

	wg     sync.WaitGroup
	closed bool

	shutdown chan struct{}
	lock     sync.Mutex
}

func (s *Service) Port() int {
	if addr, ok := s.listener.Addr().(*net.TCPAddr); ok {
		return addr.Port
	}

	return 0
}

func NewService(addr string, reg Storage) (*Service, error) {
	l, err := net.Listen("tcp", addr)

	if err != nil {
		return nil, err
	}

	debugf("start service...\n")
	s := &Service{
		Address:  addr,
		Registry: reg,
		listener: l,
		shutdown: make(chan struct{}),
	}

	s.wg.Add(1)

	return s, nil
}

func NewMemService(addr string) (*Service, error) {
	return NewService(addr, NewMemRegistry())
}

func (s *Service) Close() error {
	debugf("beginning shutdown\n")
	s.closed = true
	close(s.shutdown)
	s.listener.Close()
	s.wg.Wait()
	debugf("finished shutdown\n")
	return nil
}

func (s *Service) AcceptInsecure() error {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		s.wg.Add(1)
		go s.acceptMux(conn)
	}

	return nil
}

func (s *Service) Accept() error {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}

		sec, err := seconn.NewServer(conn)
		if err != nil {
			return err
		}

		s.wg.Add(1)
		go s.acceptMux(sec)
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

type clientEphemeralInfo struct {
	lwt *Message
}

type clientData struct {
	parent     net.Conn
	session    *yamux.Session
	inflight   map[MessageId]*Delivery
	ephemerals map[string]*clientEphemeralInfo
	closed     bool
	done       chan struct{}
	lwt        *Message
}

func (s *Service) cleanupConn(c net.Conn, data *clientData) {
	debugf("cleaning up conn to %s\n", c.RemoteAddr())

	s.lock.Lock()
	defer s.lock.Unlock()

	if data.closed {
		return
	}

	if data.lwt != nil {
		debugf("injecting lwt to %s\n", data.lwt.ReplyTo)
		name := data.lwt.ReplyTo
		data.lwt.ReplyTo = ""
		err := s.Registry.Push(name, data.lwt)

		if !errors.Equal(err, ENoMailbox) {
			debugf("lwt injection error: %s\n", err)
		}
	}

	close(data.done)

	if data.inflight != nil {
		for _, del := range data.inflight {
			del.Nack()
		}

		data.inflight = nil
	}

	for name, info := range data.ephemerals {
		s.Registry.Abandon(name)
		if info.lwt != nil {
			if info.lwt.ReplyTo == "" {
				continue
			}

			debugf("injecting ephemeral lwt to %s\n", info.lwt.ReplyTo)
			name := info.lwt.ReplyTo
			info.lwt.ReplyTo = ""
			err := s.Registry.Push(name, info.lwt)
			if !errors.Equal(err, ENoMailbox) {
				debugf("lwt ephemeral injection error: %#v\n", err)
			}
		}
	}

	data.session.Close()

	data.closed = true
}

type acceptStream struct {
	stream *yamux.Stream
	err    error
}

func (s *Service) acceptMux(c net.Conn) {
	defer s.wg.Done()

	session, err := yamux.Server(c, muxConfig)
	if err != nil {
		if eofish(err) {
			return
		}

		panic(err)
	}

	defer session.Close()

	acs := make(chan acceptStream, 1)

	debugf("new session for %s\n", c.RemoteAddr())

	data := &clientData{
		parent:     c,
		session:    session,
		inflight:   make(map[MessageId]*Delivery),
		ephemerals: make(map[string]*clientEphemeralInfo),
		done:       make(chan struct{}),
	}

	for {
		go func() {
			stream, err := session.AcceptStream()
			acs <- acceptStream{stream, err}
		}()

		select {
		case <-s.shutdown:
			session.Close()
			s.cleanupConn(c, data)
			return
		case ac := <-acs:
			if ac.err != nil {
				if eofish(ac.err) {
					debugf("eof detected starting a new stream\n")
					s.cleanupConn(c, data)
					return
				}

				panic(ac.err)
			}

			go s.handle(c, ac.stream, data)
		}
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

			err = s.handleAbandon(c, msg, data)
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

			err = s.handlePush(c, msg, data)
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

	data.ephemerals[msg.Name] = &clientEphemeralInfo{}

	s.lock.Unlock()

	_, err = c.Write([]byte{uint8(SuccessType)})
	return err
}

func (s *Service) handleAbandon(c net.Conn, msg *Abandon, data *clientData) error {
	err := s.Registry.Abandon(msg.Name)
	if err != nil {
		return err
	}

	if info, ok := data.ephemerals[msg.Name]; ok {
		if info.lwt != nil {
			debugf("injecting ephemeral lwt 2 to %s\n", info.lwt.ReplyTo)
			name := info.lwt.ReplyTo
			info.lwt.ReplyTo = ""
			err := s.Registry.Push(name, info.lwt)
			if !errors.Equal(err, ENoMailbox) {
				debugf("lwt ephemeral injection 2 error: %#v\n", err)
			}
		}
	}

	_, err = c.Write([]byte{uint8(SuccessType)})
	return err
}

func (s *Service) handlePoll(c net.Conn, msg *Poll, data *clientData) error {
	var ret PollResult

	if msg.Name == ":lwt" {
		ret.Message = data.lwt
	} else {
		val, err := s.Registry.Poll(msg.Name)
		if err != nil {
			return err
		}

		if val != nil {
			data.inflight[val.Message.MessageId] = val
			ret.Message = val.Message
		}
	}

	c.Write([]byte{uint8(PollResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&ret)
}

func (s *Service) handleLongPoll(c net.Conn, msg *LongPoll, data *clientData) error {
	debugf("handleLongPoll for %#v\n", s.Registry)

	var ret PollResult

	if msg.Name == ":lwt" {
		ret.Message = data.lwt
	} else {
		dur, err := time.ParseDuration(msg.Duration)
		if err != nil {
			return err
		}

		val, err := s.Registry.LongPollCancelable(msg.Name, dur, data.done)
		if err != nil {
			return err
		}

		if val != nil {
			debugf("inflight for %s: %#v\n", data.parent.RemoteAddr(), data)
			data.inflight[val.Message.MessageId] = val
			ret.Message = val.Message
		}
	}

	c.Write([]byte{uint8(PollResultType)})
	enc := codec.NewEncoder(c, &msgpack)
	return enc.Encode(&ret)
}

func (s *Service) setupLWT(msg *Message, data *clientData) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if msg.CorrelationId == "" {
		data.lwt = msg
	} else {
		info, ok := data.ephemerals[msg.CorrelationId]
		if !ok {
			return errors.Subject(ENoMailbox, msg.CorrelationId)
		}

		info.lwt = msg
	}

	return nil
}

var ErrUknownSystemMailbox = errors.New("unknown system mailbox")

func (s *Service) handleInternal(c net.Conn, msg *Push, data *clientData) error {
	var err error

	switch msg.Name {
	case ":lwt":
		debugf("%s: setup LWT", s.Address)
		err = s.setupLWT(msg.Message, data)
	case ":publish", ":subscribe":
		err = s.Registry.Push(msg.Name, msg.Message)
	default:
		err = errors.Subject(ErrUknownSystemMailbox, msg.Name)
	}

	return err
}

func (s *Service) handlePush(c net.Conn, msg *Push, data *clientData) error {
	debugf("%s: handlePush for %#v\n", s.Address, s.Registry)
	if msg.Name[0] == ':' {
		err := s.handleInternal(c, msg, data)
		if err != nil {
			return err
		}
	} else {
		err := s.Registry.Push(msg.Name, msg.Message)
		if err != nil {
			return err
		}

		debugf("%s: sending success\n", s.Address)
	}

	_, err := c.Write([]byte{uint8(SuccessType)})
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
	conn   net.Conn
	sess   *yamux.Session
	addr   string
	secure bool
	lwt    *Message
}

func NewClient(addr string) (*Client, error) {
	cl := &Client{
		addr:   addr,
		secure: true,
	}

	cl.Session()

	return cl, nil
}

func NewInsecureClient(addr string) (*Client, error) {
	cl := &Client{addr: addr}

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

		if c.secure {
			sec, err := seconn.NewClient(s)
			if err != nil {
				return nil, err
			}

			c.conn = sec
		} else {
			c.conn = s
		}

		sess, err := yamux.Client(c.conn, muxConfig)
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

func (c *Client) LongPollCancelable(name string, til time.Duration, done chan struct{}) (*Delivery, error) {
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

	delivered := make(chan struct{})

	buf := []byte{0}

	go func() {
		_, err = io.ReadFull(s, buf)
		close(delivered)
	}()

	select {
	case <-done:
		return nil, nil
	case <-delivered:
		// do the rest
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
