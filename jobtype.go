package jobqueue

import (
	"reflect"
)

// ReflectJobTypeOfPayload creates a job type string by using reflection on payload.
// The job type string starts with the package import path of the type
// followed by a point and the type name.
// Pointer types will be dereferenced.
// A nil payload returns an empty string.
func ReflectJobTypeOfPayload(payload any) string {
	return JobTypeOfPayloadType(reflect.TypeOf(payload))
}

// JobTypeOfPayloadType creates a job type string for a given payload reflect.Type
// The job type string starts with the package import path of the type
// followed by a point and the type name.
// Pointer types will be dereferenced.
// A nil payloadType returns an empty string.
func JobTypeOfPayloadType(payloadType reflect.Type) string {
	if payloadType == nil {
		return ""
	}
	for payloadType.Kind() == reflect.Pointer {
		payloadType = payloadType.Elem()
	}
	return payloadType.PkgPath() + "." + payloadType.Name()
}
