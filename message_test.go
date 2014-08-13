package vega

import "testing"

func TestMailboxHeaders(t *testing.T) {
	m := &Message{}

	m.AddHeader("age", 34)

	v, ok := m.GetHeader("age")
	if !ok {
		t.Fatal("missing header")
	}

	if v.(int) != 34 {
		t.Fatal("value not properly held")
	}
}

func TestMailboxGetEmpty(t *testing.T) {
	m := &Message{}

	_, ok := m.GetHeader("age")
	if ok {
		t.Fatal("missing header")
	}
}
