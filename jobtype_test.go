package jobqueue_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/domonda/go-jobqueue"
)

func TestJobTypeOfPayloadType(t *testing.T) {
	tests := []struct {
		name     string
		typ      reflect.Type
		expected string
	}{
		{"named struct from stdlib", reflect.TypeFor[time.Time](), "time.Time"},
		{"pointer is dereferenced", reflect.TypeFor[*time.Time](), "time.Time"},
		{"double pointer is dereferenced", reflect.TypeFor[**time.Time](), "time.Time"},
		{"another stdlib type", reflect.TypeFor[bytes.Buffer](), "bytes.Buffer"},
		{"builtin int has empty package path", reflect.TypeFor[int](), ".int"},
		{"builtin string has empty package path", reflect.TypeFor[string](), ".string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, jobqueue.JobTypeOfPayloadType(tt.typ))
		})
	}
}

func TestReflectJobTypeOfPayload(t *testing.T) {
	assert.Equal(t, "time.Time", jobqueue.ReflectJobTypeOfPayload(time.Time{}))
	assert.Equal(t, "time.Time", jobqueue.ReflectJobTypeOfPayload(&time.Time{}), "pointer payload is dereferenced")
	assert.Equal(t, "bytes.Buffer", jobqueue.ReflectJobTypeOfPayload(bytes.Buffer{}))

	// ReflectJobTypeOfPayload is just JobTypeOfPayloadType applied to the
	// payload's reflect.Type; the two must agree for the same value.
	assert.Equal(t,
		jobqueue.JobTypeOfPayloadType(reflect.TypeFor[bytes.Buffer]()),
		jobqueue.ReflectJobTypeOfPayload(bytes.Buffer{}),
	)
}
