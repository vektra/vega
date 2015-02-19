package testbus

import (
	"fmt"

	"github.com/vektra/vega"
)

type TestBus struct {
	Service *vega.Service
}

func NewTestBus() *TestBus {
	serv, err := vega.NewMemService("127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	tb := &TestBus{serv}

	go serv.Accept()

	return tb
}

func (tb *TestBus) Close() {
	tb.Service.Close()
}

func (tb *TestBus) Port() int {
	return tb.Service.Port()
}

func (tb *TestBus) Client() *vega.FeatureClient {
	client, err := vega.NewInsecureClient(fmt.Sprintf("127.0.0.1:%d", tb.Port()))
	if err != nil {
		panic(err)
	}

	return &FeatureClient{
		Client: client,
	}
}
