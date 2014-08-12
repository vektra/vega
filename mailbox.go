package mailbox

type MemMailbox struct {
	name     string
	values   []*Message
	inflight map[string]*Message
	watchers []chan *Message
}

func NewMemMailbox(name string) Mailbox {
	return &MemMailbox{name, nil, make(map[string]*Message), nil}
}

func (mm *MemMailbox) Ack(id string) error {
	if _, ok := mm.inflight[id]; ok {
		delete(mm.inflight, id)
		return nil
	}

	return EUnknownMessage
}

func (mm *MemMailbox) Nack(id string) error {
	if c, ok := mm.inflight[id]; ok {
		delete(mm.inflight, id)
		mm.values = append([]*Message{c}, mm.values...)
		return nil
	}

	return EUnknownMessage
}

func (mm *MemMailbox) Abandon() error {
	mm.values = nil
	for _, w := range mm.watchers {
		w <- nil
	}

	return nil
}

func (mm *MemMailbox) Poll() (*Message, error) {
	if len(mm.values) > 0 {
		val := mm.values[0]
		mm.values = mm.values[1:]

		if val.MessageId == "" {
			val.MessageId = NextMessageID()
		}

		mm.inflight[val.MessageId] = val
		return val, nil
	}

	return nil, nil
}

func (mm *MemMailbox) Push(value *Message) error {
	if len(mm.watchers) > 0 {
		watch := mm.watchers[0]
		mm.watchers = mm.watchers[1:]

		if value.MessageId == "" {
			value.MessageId = NextMessageID()
		}

		mm.inflight[value.MessageId] = value

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
		Size:     len(mm.values),
		InFlight: len(mm.inflight),
	}
}
