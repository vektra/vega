package mailbox

import "time"

type clusterNode struct {
	router *Router
	local  *Registry
	disk   *diskStorage
}

func NewClusterNode(path string, router *Router) (*clusterNode, error) {
	disk, err := NewDiskStorage(path)
	if err != nil {
		return nil, err
	}

	return &clusterNode{
		disk:   disk,
		local:  NewRegistry(disk.Mailbox),
		router: router,
	}, nil
}

func NewMemClusterNode(path string) (*clusterNode, error) {
	return NewClusterNode(path, MemRouter())
}

func (cn *clusterNode) AddRoute(name string, s Storage) {
	cn.router.Add(name, s)
}

func (cn *clusterNode) Close() error {
	return cn.disk.Close()
}

func (cn *clusterNode) Declare(name string) error {
	cn.local.Declare(name)
	cn.router.Add(name, cn.local)
	return nil
}

func (cn *clusterNode) Push(name string, msg *Message) error {
	return cn.router.Push(name, msg)
}

func (cn *clusterNode) Poll(name string) (*Message, error) {
	return cn.local.Poll(name)
}

func (cn *clusterNode) LongPoll(name string, til time.Duration) (*Message, error) {
	return cn.local.LongPoll(name, til)
}
