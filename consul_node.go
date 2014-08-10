package mailbox

import "fmt"

type ConsulClusterNode struct {
	*clusterNode

	Config *ConsulNodeConfig

	service *Service
}

const DefaultPort = 8475

const DefaultPath = "/var/lib/mailbox"

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

	return &ConsulClusterNode{cn, config, serv}, err
}

func (cn *ConsulClusterNode) Accept() error {
	return cn.service.Accept()
}
