package mailbox

type MailboxStats struct {
	Size int
}

type Mailbox interface {
	Push([]byte) error
	Poll() ([]byte, bool)
	AddWatcher() <-chan []byte
	Stats() *MailboxStats
}

type MemMailbox struct {
	values   [][]byte
	watchers []chan []byte
}

func NewMemMailbox() Mailbox {
	return &MemMailbox{nil, nil}
}

func (mm *MemMailbox) Poll() ([]byte, bool) {
	if len(mm.values) > 0 {
		val := mm.values[0]
		mm.values = mm.values[1:]
		return val, true
	}

	return nil, false
}

func (mm *MemMailbox) Push(value []byte) error {
	if len(mm.watchers) > 0 {
		watch := mm.watchers[0]
		mm.watchers = mm.watchers[1:]

		watch <- value

		return nil
	}

	mm.values = append(mm.values, value)

	return nil
}

func (mm *MemMailbox) AddWatcher() <-chan []byte {
	indicator := make(chan []byte, 1)

	mm.watchers = append(mm.watchers, indicator)

	return indicator
}

func (mm *MemMailbox) Stats() *MailboxStats {
	return &MailboxStats{
		Size: len(mm.values),
	}
}
