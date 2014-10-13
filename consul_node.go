package vega

import "fmt"

type ConsulClusterNode struct {
	*clusterNode

	Config *ConsulNodeConfig

	routes  *consulRoutingTable
	service *Service
}

const DefaultClusterPort = 8476

const DefaultPath = "/var/lib/vega"

type ConsulNodeConfig struct {
	AdvertiseAddr string
	ListenPort    int
	DataPath      string
}

func (cn *ConsulNodeConfig) Normalize() error {
	if cn.ListenPort == 0 {
		cn.ListenPort = DefaultPort
	}

	if cn.AdvertiseAddr == "" {
		ip, err := GetPrivateIP()
		if err != nil {
			cn.AdvertiseAddr = "127.0.0.1"
		} else {
			cn.AdvertiseAddr = ip.String()
		}
	}

	if cn.DataPath == "" {
		cn.DataPath = DefaultPath
	}

	return nil
}

func (cn *ConsulNodeConfig) ListenAddr() string {
	return fmt.Sprintf(":%d", cn.ListenPort)
}

func (cn *ConsulNodeConfig) AdvertiseID() string {
	return fmt.Sprintf("%s:%d", cn.AdvertiseAddr, cn.ListenPort)
}

func NewConsulClusterNode(config *ConsulNodeConfig) (*ConsulClusterNode, error) {
	if config == nil {
		config = &ConsulNodeConfig{}
	}

	err := config.Normalize()
	if err != nil {
		return nil, err
	}

	ct, err := NewConsulRoutingTable(config.AdvertiseID())
	if err != nil {
		return nil, err
	}

	cn, err := NewClusterNode(config.DataPath, NewRouter(ct))
	if err != nil {
		return nil, err
	}

	serv, err := NewService(config.ListenAddr(), cn)
	if err != nil {
		cn.Close()
		return nil, err
	}

	ccn := &ConsulClusterNode{
		clusterNode: cn,
		Config:      config,
		routes:      ct,
		service:     serv,
	}

	for _, name := range cn.disk.MailboxNames() {
		ccn.Declare(name)
	}

	return ccn, nil
}

func (cn *ConsulClusterNode) Cleanup() error {
	return cn.routes.Cleanup()
}

func (cn *ConsulClusterNode) Accept() error {
	return cn.service.Accept()
}

func (cn *ConsulClusterNode) Close() error {
	cn.clusterNode.Close()
	return cn.service.Close()
}
