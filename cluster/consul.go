package cluster

import (
	"github.com/armon/consul-api"
)

func NewConsulClient(token string) *consulapi.Client {
	config := consulapi.DefaultConfig()
	config.Token = token

	client, _ := consulapi.NewClient(config)
	return client
}
