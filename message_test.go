package vega

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMailboxHeaders(t *testing.T) {
	m := &Message{}

	m.AddHeader("age", 34)

	v, ok := m.GetHeader("age")
	if !ok {
		t.Fatal("missing header")
	}

	assert.Equal(t, 34, v.(int))
}

func TestMailboxGetEmpty(t *testing.T) {
	m := &Message{}

	_, ok := m.GetHeader("age")
	assert.False(t, ok)
}
