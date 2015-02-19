package disk

import (
	"errors"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/ugorji/go/codec"
	"github.com/vektra/vega"
)

var msgpack codec.MsgpackHandle

var ECorruptMailbox = errors.New("corrupt mailbox metadata")

type Storage struct {
	db *bolt.DB

	lock sync.Mutex
}

const LRUCacheSize = 100 * 1048576

func NewDiskStorage(path string) (*Storage, error) {
	db, err := bolt.Open(filepath.Join(path, "vega.db"), 0600, nil)
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
	Mailboxes map[string]struct{}
}

func diskDataMarshal(v interface{}) ([]byte, error) {
	var out []byte
	err := codec.NewEncoderBytes(&out, &msgpack).Encode(v)
	return out, err
}

func diskDataUnmarshal(data []byte, v interface{}) error {
	return codec.NewDecoderBytes(data, &msgpack).Decode(v)
}

var (
	cSystem        = []byte(":system:")
	cMInfo         = []byte(":info:")
	cMessagePrefix = []byte("m-")
)

func (d *Storage) Mailbox(name string) vega.Mailbox {
	d.lock.Lock()
	defer d.lock.Unlock()

	db := d.db

	key := []byte(":info:")

	err := db.Update(func(tx *bolt.Tx) error {
		buk, err := tx.CreateBucketIfNotExists(cSystem)
		if err != nil {
			return err
		}

		data := buk.Get(key)

		var header infoHeader

		if len(data) != 0 {
			err := diskDataUnmarshal(data, &header)
			if err != nil {
				return err
			}
		}

		if header.Mailboxes == nil {
			header.Mailboxes = map[string]struct{}{
				(name): struct{}{},
			}
		} else {
			header.Mailboxes[name] = struct{}{}
		}

		headerData, err := diskDataMarshal(&header)
		if err != nil {
			return err
		}

		return buk.Put(key, headerData)
	})

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

	db := d.db

	key := []byte(":info:")

	var header infoHeader

	err := db.Update(func(tx *bolt.Tx) error {
		buk, err := tx.CreateBucketIfNotExists(cSystem)
		if err != nil {
			return err
		}

		data := buk.Get(key)

		if len(data) != 0 {
			return diskDataUnmarshal(data, &header)
		}

		return nil
	})

	if err != nil {
		panic(err)
	}

	mailboxes := make([]string, 0, len(header.Mailboxes))
	for name := range header.Mailboxes {
		mailboxes = append(mailboxes, name)
	}
	return mailboxes
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

	db := m.disk.db

	return db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(m.prefix)
		if err != nil && err != bolt.ErrBucketNotFound {
			return err
		}

		buk := tx.Bucket(cSystem)

		key := []byte(":info:")

		data := buk.Get(key)

		if len(data) > 0 {
			var header infoHeader

			err = diskDataUnmarshal(data, &header)
			if err != nil {
				return err
			}

			if header.Mailboxes != nil {
				delete(header.Mailboxes, string(m.prefix))

				data, err = diskDataMarshal(&header)
				if err != nil {
					return err
				}

				buk.Put(key, data)
			}
		}

		return nil
	})
}

func (m *diskMailbox) Poll() (*vega.Message, error) {
	m.Lock()
	defer m.Unlock()

	db := m.disk.db

	var data []byte

	err := db.Update(func(tx *bolt.Tx) error {
		buk := tx.Bucket(m.prefix)
		if buk == nil {
			return nil
		}

		info := buk.Get(cMInfo)

		if len(info) == 0 {
			return nil
		}

		var header mailboxHeader

		err := diskDataUnmarshal(info, &header)
		if err != nil {
			return err
		}

		var idx int

		if len(header.DCMessages) > 0 {
			idx = header.DCMessages[0]
			header.DCMessages = header.DCMessages[1:]
		} else {
			if header.Size == 0 {
				return nil
			}

			idx = header.ReadIndex
			header.ReadIndex++
			header.Size--
		}

		key := append(cMessagePrefix, []byte(strconv.Itoa(idx))...)

		data = buk.Get(key)
		if data == nil {
			return ECorruptMailbox
		}

		header.InFlight++

		headerData, err := diskDataMarshal(&header)
		if err != nil {
			return err
		}

		return buk.Put(cMInfo, headerData)
	})

	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	return vega.DecodeMessage(data), nil
}

func (m *diskMailbox) Ack(id vega.MessageId) error {
	m.Lock()
	defer m.Unlock()

	db := m.disk.db

	return db.Update(func(tx *bolt.Tx) error {

		buk := tx.Bucket(m.prefix)

		data := buk.Get(cMInfo)

		if len(data) == 0 {
			return vega.EUnknownMessage
		}

		var header mailboxHeader

		err := diskDataUnmarshal(data, &header)
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

		key := append(cMessagePrefix, []byte(idxStr)...)

		err = buk.Delete(key)
		if err != nil {
			return err
		}

		header.InFlight--

		headerData, err := diskDataMarshal(&header)
		if err != nil {
			return err
		}

		return buk.Put(cMInfo, headerData)
	})
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

	db := m.disk.db

	return db.Update(func(tx *bolt.Tx) error {
		var header mailboxHeader

		buk := tx.Bucket(m.prefix)

		data := buk.Get(cMInfo)
		if data == nil {
			return vega.EUnknownMessage
		}

		diskDataUnmarshal(data, &header)

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

		return buk.Put(cMInfo, headerData)
	})
}

func (m *diskMailbox) Push(value *vega.Message) error {
	m.Lock()
	defer m.Unlock()

	db := m.disk.db

	return db.Update(func(tx *bolt.Tx) error {

		var header mailboxHeader

		buk, err := tx.CreateBucketIfNotExists(m.prefix)
		if err != nil {
			return err
		}

		data := buk.Get(cMInfo)
		if len(data) > 0 {
			err := diskDataUnmarshal(data, &header)
			if err != nil {
				return err
			}
		}

		idxStr := strconv.Itoa(header.WriteIndex)

		if value.MessageId == "" {
			value.MessageId = vega.NextMessageID()
		}

		value.MessageId = value.MessageId.AppendLocalIndex(idxStr)

		key := append(cMessagePrefix, []byte(idxStr)...)

		buk.Put(key, value.AsBytes())

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

		err = buk.Put(cMInfo, headerData)
		if err != nil {
			return err
		}

		if watch != nil {
			watch.indicator <- value
			close(watch.indicator)
		}

		return nil
	})
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

	db := m.disk.db

	var header mailboxHeader

	db.View(func(tx *bolt.Tx) error {
		buk := tx.Bucket(m.prefix)
		if buk == nil {
			return nil
		}

		data := buk.Get(cMInfo)
		diskDataUnmarshal(data, &header)
		return nil
	})

	return &vega.MailboxStats{
		Size:     header.Size + len(header.DCMessages),
		InFlight: header.InFlight,
	}
}
