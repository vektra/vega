package mailbox

type MemRouteTable map[string]Pusher

func (ht MemRouteTable) Set(name string, st Pusher) error {
	ht[name] = st
	return nil
}

func (ht MemRouteTable) Get(name string) (Pusher, bool) {
	s, ok := ht[name]
	return s, ok
}

type Router struct {
	routes RouteTable
}

func NewRouter(rt RouteTable) *Router {
	return &Router{rt}
}

func MemRouter() *Router {
	return NewRouter(make(MemRouteTable))
}

func (r *Router) Add(name string, reg Pusher) {
	r.routes.Set(name, reg)
}

func (r *Router) DiscoverEndpoint(name string) (Pusher, bool) {
	reg, ok := r.routes.Get(name)

	return reg, ok
}

func (r *Router) Push(name string, body *Message) error {
	if storage, ok := r.routes.Get(name); ok {
		debugf("Routing %s to %#v\n", name, storage)
		return storage.Push(name, body)
	}

	return ENoMailbox
}
