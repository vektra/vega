package vega

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vektra/neko"
)

func TestSubscription(t *testing.T) {
	n := neko.Start(t)

	n.It("matches an explicit path", func() {
		sub := ParseSubscription("foo")
		assert.True(t, sub.Match("foo"))
		assert.False(t, sub.Match("bar"))
		assert.False(t, sub.Match("foo2"))
	})

	n.It("uses + to match any segment", func() {
		sub := ParseSubscription("+")
		assert.True(t, sub.Match("foo"))
		assert.False(t, sub.Match("foo/bar"))
	})

	n.It("uses + to match a segment after literal", func() {
		sub := ParseSubscription("foo/+")
		assert.True(t, sub.Match("foo/bar"))
		assert.False(t, sub.Match("foo"))
		assert.False(t, sub.Match("bar/foo"))
		assert.False(t, sub.Match("bar/foo/baz"))
	})

	n.It("uses # to match any tail", func() {
		sub := ParseSubscription("foo/#")
		assert.True(t, sub.Match("foo/bar"))
		assert.True(t, sub.Match("foo/bar/baz"))
		assert.True(t, sub.Match("foo/bar/baz/quz"))
		assert.False(t, sub.Match("foo"))
		assert.False(t, sub.Match("bar"))
		assert.False(t, sub.Match("bar/foo"))
		assert.False(t, sub.Match("bar/qux"))
	})

	n.Meow()
}
