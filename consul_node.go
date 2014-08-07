package mailbox

type consulClusterNode struct {
	*clusterNode

	service *Service
}

func NewConsulClusterNode(id, port, path string) (*consulClusterNode, error) {
	ct, err := NewConsulRoutingTable(id)
	if err != nil {
		return nil, err
	}

	cn, err := NewClusterNode(path, NewRouter(ct))
	if err != nil {
		return nil, err
	}

	serv, err := NewService(port, cn)
	if err != nil {
		cn.Close()
		return nil, err
	}

	return &consulClusterNode{cn, serv}, err
}
