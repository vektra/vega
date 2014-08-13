package vega

type multiPusher struct {
	pushers []Pusher
}

func NewMultiPusher() *multiPusher {
	return &multiPusher{}
}

func (mp *multiPusher) Add(p Pusher) {
	mp.pushers = append(mp.pushers, p)
}

func (mp *multiPusher) Push(name string, msg *Message) error {
	var final error

	for _, p := range mp.pushers {
		err := p.Push(name, msg)
		if err != nil {
			final = err
		}
	}

	return final
}
