package vega

import "strings"

type Subscription struct {
	Pattern string
	Parts   []string
	Strict  bool
	Mailbox string
}

func ParseSubscription(pattern string) *Subscription {
	parts := strings.Split(pattern, "/")

	var strict bool

	if parts[len(parts)-1] == "#" {
		parts[len(parts)-1] = "+"
	} else {
		strict = true
	}

	return &Subscription{pattern, parts, strict, ""}
}

func (s *Subscription) Match(lit string) bool {
	parts := strings.Split(lit, "/")

	if len(parts) != len(s.Parts) {
		if s.Strict {
			return false
		}

		if len(parts) < len(s.Parts) {
			return false
		}

		parts = parts[:len(s.Parts)]
	}

	for i, against := range s.Parts {
		concrete := parts[i]

		if against == "+" || against == concrete {
			continue
		}

		return false
	}

	return true
}
