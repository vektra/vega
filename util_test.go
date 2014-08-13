package vega

import "testing"

func BenchmarkGenerateUUIDFast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		generateUUID()
	}
}

func BenchmarkGenerateUUIDSecure(b *testing.B) {
	for i := 0; i < b.N; i++ {
		generateUUIDSecure()
	}
}

func BenchmarkNextMessageID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NextMessageID()
	}
}
