package mailbox

import "sync"

type reliableMessage struct {
	queue   string
	message *Message
}

type reliableStorage struct {
	Storage

	bufferLock sync.Mutex
	buffer     []*reliableMessage
}

func NewReliableStorage(s Storage) *reliableStorage {
	return &reliableStorage{
		Storage: s,
		buffer:  nil,
	}
}

func (rs *reliableStorage) BufferedMessages() int {
	return len(rs.buffer)
}

func (rs *reliableStorage) Retry() error {
	rs.bufferLock.Lock()

	for idx, msg := range rs.buffer {
		err := rs.Storage.Push(msg.queue, msg.message)
		if err != nil {
			if idx > 0 {
				rs.buffer = rs.buffer[idx:]
				return nil
			}
		}
	}

	rs.buffer = nil

	rs.bufferLock.Unlock()

	return nil
}

func (rs *reliableStorage) Push(name string, msg *Message) error {
	if len(rs.buffer) > 0 {
		if err := rs.Retry(); err != nil {
			rs.bufferLock.Lock()
			rs.buffer = append(rs.buffer, &reliableMessage{name, msg})
			rs.bufferLock.Unlock()

			return nil
		}
	}

	if err := rs.Storage.Push(name, msg); err != nil {
		rs.bufferLock.Lock()
		rs.buffer = append(rs.buffer, &reliableMessage{name, msg})
		rs.bufferLock.Unlock()
	}

	return nil
}
