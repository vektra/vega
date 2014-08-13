package vega

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/ugorji/go/codec"
)

var ECorruptMailbox = errors.New("corrupt mailbox metadata")

type diskStorage struct {
	db *levigo.DB

	lock sync.Mutex
}

func NewDiskStorage(path string) (*diskStorage, error) {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &diskStorage{db: db}, nil
}

func (d *diskStorage) Close() error {
	d.db.Close()
	return nil
}

type diskMailbox struct {
	sync.Mutex

	disk     *diskStorage
	prefix   []byte
	watchers []*watchChannel
}

type infoHeader struct {
	Mailboxes []string
}

func diskDataMarshal(v interface{}) ([]byte, error) {
	var out []byte
	err := codec.NewEncoderBytes(&out, &msgpack).Encode(v)
	return out, err
}

func diskDataUnmarshal(data []byte, v interface{}) error {
	return codec.NewDecoderBytes(data, &msgpack).Decode(v)
}

func (d *diskStorage) Mailbox(name string) Mailbox {
	d.lock.Lock()
	defer d.lock.Unlock()

	ro := levigo.NewReadOptions()

	db := d.db

	key := []byte(":info:")

	data, err := db.Get(ro, key)
	if err != nil {
		panic(err)
	}

	var header infoHeader

	if len(data) != 0 {
		err = diskDataUnmarshal(data, &header)
		if err != nil {
			panic(err)
		}
	}

	header.Mailboxes = append(header.Mailboxes, name)

	wo := levigo.NewWriteOptions()

	headerData, err := diskDataMarshal(&header)
	if err != nil {
		panic(err)
	}

	wo.SetSync(true)

	err = db.Put(wo, key, headerData)
	if err != nil {
		panic(err)
	}

	return &diskMailbox{
		disk:     d,
		prefix:   []byte(name),
		watchers: nil,
	}
}

func (d *diskStorage) MailboxNames() []string {
	d.lock.Lock()
	defer d.lock.Unlock()

	ro := levigo.NewReadOptions()

	db := d.db

	key := []byte(":info:")

	data, err := db.Get(ro, key)
	if err != nil {
		return nil
	}

	var header infoHeader

	if len(data) != 0 {
		err = diskDataUnmarshal(data, &header)
		if err != nil {
			return nil
		}
	}

	return header.Mailboxes
}

type mailboxHeader struct {
	AckIndex, ReadIndex, WriteIndex, Size int
	InFlight                              int

	DCMessages []int
}

func (m *diskMailbox) Abandon() error {
	m.Lock()
	defer m.Unlock()

	for _, w := range m.watchers {
		w.indicator <- nil
	}

	ro := levigo.NewReadOptions()

	db := m.disk.db

	data, err := db.Get(ro, m.prefix)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	var header mailboxHeader

	err = diskDataUnmarshal(data, &header)
	if err != nil {
		return err
	}

	batch := levigo.NewWriteBatch()

	for i := header.AckIndex; i < header.ReadIndex+header.Size; i++ {
		key := append(m.prefix, []byte(strconv.Itoa(i))...)

		batch.Delete(key)
	}

	batch.Delete(m.prefix)

	wo := levigo.NewWriteOptions()
	wo.SetSync(true)

	err = db.Write(wo, batch)
	if err != nil {
		return err
	}

	return nil
}

func (m *diskMailbox) Poll() (*Message, error) {
	m.Lock()
	defer m.Unlock()

	ro := levigo.NewReadOptions()

	db := m.disk.db

	data, err := db.Get(ro, m.prefix)
	if err != nil {
		return nil, nil
	}

	if len(data) == 0 {
		return nil, nil
	}

	var header mailboxHeader

	err = diskDataUnmarshal(data, &header)
	if err != nil {
		return nil, err
	}

	var idx int

	if len(header.DCMessages) > 0 {
		idx = header.DCMessages[0]
		header.DCMessages = header.DCMessages[1:]
	} else {
		if header.Size == 0 {
			return nil, nil
		}

		idx = header.ReadIndex
		header.ReadIndex++
		header.Size--
	}

	key := append(m.prefix, []byte(strconv.Itoa(idx))...)

	data, err = db.Get(ro, key)
	if err != nil {
		return nil, err
	}

	wo := levigo.NewWriteOptions()

	header.InFlight++

	headerData, err := diskDataMarshal(&header)
	if err != nil {
		return nil, err
	}

	err = db.Put(wo, m.prefix, headerData)
	if err != nil {
		return nil, err
	}

	return DecodeMessage(data), nil
}

func (id MessageId) LocalIndex() string {
	colonPos := strings.LastIndex(string(id), ":")
	if colonPos == -1 {
		return ""
	}

	return string(id[colonPos+1:])
}

func (id MessageId) AppendLocalIndex(idxStr string) MessageId {
	return id + ":" + MessageId(idxStr)
}

func (m *diskMailbox) Ack(id MessageId) error {
	m.Lock()
	defer m.Unlock()

	ro := levigo.NewReadOptions()

	db := m.disk.db

	data, err := db.Get(ro, m.prefix)
	if err != nil {
		return EUnknownMessage
	}

	if len(data) == 0 {
		return EUnknownMessage
	}

	var header mailboxHeader

	err = diskDataUnmarshal(data, &header)
	if err != nil {
		return ECorruptMailbox
	}

	idxStr := id.LocalIndex()
	if idxStr == "" {
		return EUnknownMessage
	}

	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return err
	}

	debugf("acking message %d (AckIndex: %d)\n", idx, header.AckIndex)

	if header.ReadIndex-header.AckIndex == 0 {
		return EUnknownMessage
	}

	if idx < header.AckIndex || idx >= header.ReadIndex {
		return EUnknownMessage
	}

	// Messages may be ack'd incontigiously. That's fine, we'll
	// just track AckIndex as the oldest un-acked message.
	if header.AckIndex == idx {
		header.AckIndex++
	}

	key := append(m.prefix, []byte(idxStr)...)

	batch := levigo.NewWriteBatch()

	batch.Delete(key)

	header.InFlight--

	headerData, err := diskDataMarshal(&header)
	if err != nil {
		return err
	}

	batch.Put(m.prefix, headerData)

	wo := levigo.NewWriteOptions()
	wo.SetSync(true)

	err = db.Write(wo, batch)
	if err != nil {
		return err
	}

	return nil
}

func (m *diskMailbox) Nack(id MessageId) error {
	m.Lock()
	defer m.Unlock()

	idxStr := id.LocalIndex()
	if idxStr == "" {
		return EUnknownMessage
	}

	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return err
	}

	ro := levigo.NewReadOptions()

	db := m.disk.db

	var header mailboxHeader

	data, err := db.Get(ro, m.prefix)
	if err == nil {
		diskDataUnmarshal(data, &header)
	}

	if idx < header.AckIndex || idx >= header.ReadIndex {
		return EUnknownMessage
	}

	header.InFlight--

	// optimization, nack'ing the last read message
	if idx == header.ReadIndex-1 {
		header.ReadIndex--
		header.Size++
	} else {
		header.DCMessages = append(header.DCMessages, idx)
	}

	headerData, err := diskDataMarshal(&header)
	if err != nil {
		return err
	}

	wo := levigo.NewWriteOptions()
	wo.SetSync(true)

	err = db.Put(wo, m.prefix, headerData)
	if err != nil {
		return err
	}

	return nil
}

func (m *diskMailbox) Push(value *Message) error {
	m.Lock()
	defer m.Unlock()

	ro := levigo.NewReadOptions()

	db := m.disk.db

	var header mailboxHeader

	data, err := db.Get(ro, m.prefix)
	if err == nil {
		if len(data) > 0 {
			err = diskDataUnmarshal(data, &header)
			if err != nil {
				return err
			}
		}
	}

	idxStr := strconv.Itoa(header.WriteIndex)

	if value.MessageId == "" {
		value.MessageId = NextMessageID()
	}

	value.MessageId = value.MessageId.AppendLocalIndex(idxStr)

	key := append(m.prefix, []byte(idxStr)...)

	batch := levigo.NewWriteBatch()

	batch.Put(key, value.AsBytes())

	var watch *watchChannel

	header.WriteIndex++

RETRY:
	if len(m.watchers) > 0 {
		watch = m.watchers[0]
		m.watchers = m.watchers[1:]

		if watch.done != nil {
			select {
			case <-watch.done:
				close(watch.indicator)
				watch = nil
				goto RETRY
			default:
			}
		}

		header.ReadIndex++
		header.InFlight++
	} else {
		header.Size++
	}

	headerData, err := diskDataMarshal(&header)
	if err != nil {
		return err
	}

	batch.Put(m.prefix, headerData)

	wo := levigo.NewWriteOptions()
	wo.SetSync(true)

	err = db.Write(wo, batch)
	if err != nil {
		return err
	}

	if watch != nil {
		watch.indicator <- value
		close(watch.indicator)
	}

	return nil
}

func (mm *diskMailbox) AddWatcher() <-chan *Message {
	mm.Lock()
	defer mm.Unlock()

	indicator := make(chan *Message, 1)

	mm.watchers = append(mm.watchers, &watchChannel{indicator, nil})

	return indicator
}

func (mm *diskMailbox) AddWatcherCancelable(done chan struct{}) <-chan *Message {
	mm.Lock()
	defer mm.Unlock()

	indicator := make(chan *Message, 1)

	mm.watchers = append(mm.watchers, &watchChannel{indicator, done})

	return indicator
}

func (m *diskMailbox) Stats() *MailboxStats {
	m.Lock()
	defer m.Unlock()

	ro := levigo.NewReadOptions()

	db := m.disk.db

	var header mailboxHeader

	data, err := db.Get(ro, m.prefix)
	if err == nil {
		diskDataUnmarshal(data, &header)
	}

	return &MailboxStats{
		Size:     header.Size + len(header.DCMessages),
		InFlight: header.InFlight,
	}
}
