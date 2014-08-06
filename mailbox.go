package mailbox

type MemMailbox struct {
	name     string
	values   []*Message
	watchers []chan *Message
}

func NewMemMailbox(name string) Mailbox {
	return &MemMailbox{name, nil, nil}
}

func (mm *MemMailbox) Poll() (*Message, error) {
	if len(mm.values) > 0 {
		val := mm.values[0]
		mm.values = mm.values[1:]
		return val, nil
	}

	return nil, nil
}

func (mm *MemMailbox) Push(value *Message) error {
	if len(mm.watchers) > 0 {
		watch := mm.watchers[0]
		mm.watchers = mm.watchers[1:]

		watch <- value

		return nil
	}

	mm.values = append(mm.values, value)

	return nil
}

func (mm *MemMailbox) AddWatcher() <-chan *Message {
	indicator := make(chan *Message, 1)

	mm.watchers = append(mm.watchers, indicator)

	return indicator
}

func (mm *MemMailbox) Stats() *MailboxStats {
	return &MailboxStats{
		Size: len(mm.values),
	}
}
