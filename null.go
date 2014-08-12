package mailbox

import "time"

type nullStorage struct{}

func (ns *nullStorage) Declare(string) error           { return nil }
func (ns *nullStorage) Abandon(string) error           { return nil }
func (ns *nullStorage) Push(string, *Message) error    { return nil }
func (ns *nullStorage) Poll(string) (*Delivery, error) { return nil, nil }
func (ns *nullStorage) LongPoll(string, time.Duration) (*Delivery, error) {
	return nil, nil
}

var NullStorage = &nullStorage{}
