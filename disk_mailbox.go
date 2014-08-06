package mailbox

import (
	"encoding/json"
	"strconv"
	"sync"

	"github.com/jmhodges/levigo"
)

type diskStorage struct {
	db *levigo.DB
}

func NewDiskStorage(path string) (*diskStorage, error) {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)

	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}

	return &diskStorage{db}, nil
}

func (d *diskStorage) Close() error {
	d.db.Close()
	return nil
}

type diskMailbox struct {
	sync.Mutex

	disk     *diskStorage
	prefix   []byte
	watchers []chan []byte
}

func (d *diskStorage) Mailbox(name string) Mailbox {
	return &diskMailbox{
		disk:     d,
		prefix:   []byte(name),
		watchers: nil,
	}
}

type mailboxHeader struct {
	ReadIndex, WriteIndex, Size int
}

func (m *diskMailbox) Poll() ([]byte, bool) {
	m.Lock()
	defer m.Unlock()

	ro := levigo.NewReadOptions()

	db := m.disk.db

	data, err := db.Get(ro, m.prefix)
	if err != nil {
		return nil, false
	}

	var header mailboxHeader

	err = json.Unmarshal(data, &header)
	if err != nil {
		return nil, false
	}

	if header.Size == 0 {
		return nil, false
	}

	key := append(m.prefix, []byte(strconv.Itoa(header.ReadIndex))...)

	data, err = db.Get(ro, key)
	if err != nil {
		panic(err)
	}

	wo := levigo.NewWriteOptions()

	err = db.Delete(wo, key)
	if err != nil {
		panic(err)
	}

	header.ReadIndex++
	header.Size--

	headerData, err := json.Marshal(&header)
	if err != nil {
		panic(err)
	}

	err = db.Put(wo, m.prefix, headerData)
	if err != nil {
		panic(err)
	}

	return data, true
}

func (m *diskMailbox) Push(value []byte) error {
	m.Lock()
	defer m.Unlock()

	if len(m.watchers) > 0 {
		watch := m.watchers[0]
		m.watchers = m.watchers[1:]

		watch <- value

		return nil
	}

	ro := levigo.NewReadOptions()

	db := m.disk.db

	var header mailboxHeader

	data, err := db.Get(ro, m.prefix)
	if err == nil {
		json.Unmarshal(data, &header)
	}

	key := append(m.prefix, []byte(strconv.Itoa(header.WriteIndex))...)

	wo := levigo.NewWriteOptions()

	err = db.Put(wo, key, value)
	if err != nil {
		return err
	}

	header.WriteIndex++
	header.Size++

	headerData, err := json.Marshal(&header)
	if err != nil {
		return err
	}

	err = db.Put(wo, m.prefix, headerData)
	if err != nil {
		panic(err)
	}

	return nil
}

func (mm *diskMailbox) AddWatcher() <-chan []byte {
	mm.Lock()
	defer mm.Unlock()

	indicator := make(chan []byte, 1)

	mm.watchers = append(mm.watchers, indicator)

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
		json.Unmarshal(data, &header)
	}

	return &MailboxStats{
		Size: header.Size,
	}
}
