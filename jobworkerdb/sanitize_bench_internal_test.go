package jobworkerdb

import (
	"strings"
	"testing"

	"github.com/domonda/go-types/notnull"
)

// BenchmarkSanitizeJSONClean measures the path taken by essentially every real
// job write: a payload that is already storable. Job payloads can be large, so
// this path must not copy the payload.
func BenchmarkSanitizeJSONClean(b *testing.B) {
	payload := notnull.JSON(`{"text":"` + strings.Repeat("clean payload text ", 55_000) + `"}`)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		sanitizeJSON(payload)
	}
}

// BenchmarkSanitizeJSONDirty measures the rewriting path for a payload with a
// single escaped zero byte in it.
func BenchmarkSanitizeJSONDirty(b *testing.B) {
	payload := notnull.JSON(`{"text":"` + strings.Repeat("dirty payload text ", 55_000) + escapedNul + `"}`)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		sanitizeJSON(payload)
	}
}
