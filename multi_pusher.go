package vega

type MultiPusher struct {
	Pushers []Pusher
}

func NewMultiPusher() *MultiPusher {
	return &MultiPusher{}
}

func (mp *MultiPusher) Add(p Pusher) {
	mp.Pushers = append(mp.Pushers, p)
}

func (mp *MultiPusher) Push(name string, msg *Message) error {
	var final error

	for _, p := range mp.Pushers {
		err := p.Push(name, msg)
		if err != nil {
			final = err
		}
	}

	return final
}
