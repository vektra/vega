package vega

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"time"
)

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

	sharedKey    []byte
	readDeadline time.Time
}

func (p *PipeConn) initialize() error {
	key := make([]byte, aes.BlockSize)

	s1 := sha256.Sum256([]byte(p.ownM))
	s2 := sha256.Sum256([]byte(p.pairM))

	XORBytes(key, s1[:aes.BlockSize], s2[:aes.BlockSize])

	p.sharedKey = key

	msg := &Message{
		ReplyTo:       p.pairM,
		Type:          "pipe/close",
		CorrelationId: p.ownM,
	}

	err := p.fc.Push(":lwt", msg)
	if err != nil {
		return err
	}

	return nil
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

type streamWrapper struct {
	net.Conn
	S cipher.Stream
}

func (r *streamWrapper) Read(dst []byte) (n int, err error) {
	n, err = r.Conn.Read(dst)
	r.S.XORKeyStream(dst[:n], dst[:n])
	return
}

func (w *streamWrapper) Write(src []byte) (n int, err error) {
	c := make([]byte, len(src))
	w.S.XORKeyStream(c, src)
	n, err = w.Conn.Write(c)
	if n != len(src) {
		if err == nil { // should never happen
			err = io.ErrShortWrite
		}
	}

	return
}

func (p *PipeConn) readBulk(msg *Message, data []byte) (int, error) {
	socketId := msg.CorrelationId

	s, err := net.Dial("tcp", socketId)
	if err != nil {
		return 0, err
	}

	block, err := aes.NewCipher(p.sharedKey)
	if err != nil {
		return 0, err
	}

	stream := cipher.NewOFB(block, msg.Body)
	s = &streamWrapper{s, stream}

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

	msg.Body = RandomIV(aes.BlockSize)

	block, err := aes.NewCipher(p.sharedKey)
	if err != nil {
		return 0, err
	}

	stream := cipher.NewOFB(block, msg.Body)
	data = &cipher.StreamReader{S: stream, R: data}

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

		debugf("successful pipe start from %s", resp.Message.ReplyTo)

		ownM := RandomMailbox()
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

		pc := &PipeConn{
			fc:    fc,
			pairM: resp.Message.ReplyTo,
			ownM:  ownM,
		}

		err = pc.initialize()
		if err != nil {
			fc.Abandon(ownM)
			return nil, err
		}

		debugf("pipe created at %s", ownM)

		return pc, nil
	}
}

func (fc *FeatureClient) ConnectPipe(name string) (*PipeConn, error) {
	ownM := RandomMailbox()
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
		debugf("waiting on %s for handshake", ownM)
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

		pc := &PipeConn{
			fc:    fc,
			pairM: resp.Message.ReplyTo,
			ownM:  ownM,
		}

		err = pc.initialize()
		if err != nil {
			fc.Abandon(ownM)
			return nil, err
		}

		return pc, nil
	}
}
