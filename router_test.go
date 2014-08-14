package vega

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouterAdd(t *testing.T) {
	r := NewMemRegistry()

	d := MemRouter()
	d.Add("a", r)

	r2, ok := d.DiscoverEndpoint("a")
	assert.True(t, ok)

	assert.True(t, r2 == r)
}

func TestRouterPush(t *testing.T) {
	r := NewMemRegistry()
	r.Declare("a")

	d := MemRouter()
	d.Add("a", r)

	msg := Msg([]byte("hello"))

	err := d.Push("a", msg)
	if err != nil {
		panic(err)
	}

	res, _ := r.Poll("a")

	assert.True(t, msg.Equal(res.Message))
}
