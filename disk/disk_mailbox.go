package disk

import (
	"errors"
	"strconv"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/ugorji/go/codec"
	"github.com/vektra/vega"
)

var msgpack codec.MsgpackHandle

var ECorruptMailbox = errors.New("corrupt mailbox metadata")

type Storage struct {
	db *levigo.DB

	lock sync.Mutex
}

const LRUCacheSize = 100 * 1048576

func NewDiskStorage(path string) (*Storage, error) {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(LRUCacheSize))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &Storage{db: db}, nil
}

func (d *Storage) Close() error {
	d.db.Close()
	return nil
}

type watchChannel struct {
	indicator chan *vega.Message
	done      chan struct{}
}

type diskMailbox struct {
	sync.Mutex

	disk     *Storage
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

func (d *Storage) Mailbox(name string) vega.Mailbox {
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

func (d *Storage) MailboxNames() []string {
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

func (m *diskMailbox) Poll() (*vega.Message, error) {
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

	return vega.DecodeMessage(data), nil
}

func (m *diskMailbox) Ack(id vega.MessageId) error {
	m.Lock()
	defer m.Unlock()

	ro := levigo.NewReadOptions()

	db := m.disk.db

	data, err := db.Get(ro, m.prefix)
	if err != nil {
		return vega.EUnknownMessage
	}

	if len(data) == 0 {
		return vega.EUnknownMessage
	}

	var header mailboxHeader

	err = diskDataUnmarshal(data, &header)
	if err != nil {
		return ECorruptMailbox
	}

	idxStr := id.LocalIndex()
	if idxStr == "" {
		return vega.EUnknownMessage
	}

	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return err
	}

	// debugf("acking message %d (AckIndex: %d)\n", idx, header.AckIndex)

	if header.ReadIndex-header.AckIndex == 0 {
		return vega.EUnknownMessage
	}

	if idx < header.AckIndex || idx >= header.ReadIndex {
		return vega.EUnknownMessage
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

func (m *diskMailbox) Nack(id vega.MessageId) error {
	m.Lock()
	defer m.Unlock()

	idxStr := id.LocalIndex()
	if idxStr == "" {
		return vega.EUnknownMessage
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
		return vega.EUnknownMessage
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

func (m *diskMailbox) Push(value *vega.Message) error {
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
		value.MessageId = vega.NextMessageID()
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

func (mm *diskMailbox) AddWatcher() <-chan *vega.Message {
	mm.Lock()
	defer mm.Unlock()

	indicator := make(chan *vega.Message, 1)

	mm.watchers = append(mm.watchers, &watchChannel{indicator, nil})

	return indicator
}

func (mm *diskMailbox) AddWatcherCancelable(done chan struct{}) <-chan *vega.Message {
	mm.Lock()
	defer mm.Unlock()

	indicator := make(chan *vega.Message, 1)

	mm.watchers = append(mm.watchers, &watchChannel{indicator, done})

	return indicator
}

func (m *diskMailbox) Stats() *vega.MailboxStats {
	m.Lock()
	defer m.Unlock()

	ro := levigo.NewReadOptions()

	db := m.disk.db

	var header mailboxHeader

	data, err := db.Get(ro, m.prefix)
	if err == nil {
		diskDataUnmarshal(data, &header)
	}

	return &vega.MailboxStats{
		Size:     header.Size + len(header.DCMessages),
		InFlight: header.InFlight,
	}
}
