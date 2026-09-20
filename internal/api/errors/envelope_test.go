package errors

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

func TestEnvelope_ErrorShapeAndContentType(t *testing.T) {
	t.Parallel()
	env := Map(&kafka.Error{Kind: kafka.KindNotFound, Resource: "topic", KafkaCode: 3, KafkaName: "UNKNOWN_TOPIC_OR_PARTITION"}, "req-1")

	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}
	errObj, ok := got["error"].(map[string]any)
	if !ok {
		t.Fatalf("envelope has no top-level \"error\" object: %s", b)
	}
	for _, field := range []string{"code", "message", "status", "requestId"} {
		if _, ok := errObj[field]; !ok {
			t.Errorf("error object is missing %q: %s", field, b)
		}
	}
	if errObj["code"] != "NOT_FOUND" {
		t.Errorf("code = %v, want NOT_FOUND", errObj["code"])
	}
	if errObj["requestId"] != "req-1" {
		t.Errorf("requestId = %v, want req-1", errObj["requestId"])
	}
	kerr, ok := errObj["kafkaError"].(map[string]any)
	if !ok || kerr["name"] != "UNKNOWN_TOPIC_OR_PARTITION" {
		t.Errorf("kafkaError = %v, want {code:3,name:UNKNOWN_TOPIC_OR_PARTITION}", errObj["kafkaError"])
	}
	if env.GetStatus() != 404 {
		t.Errorf("GetStatus() = %d, want 404", env.GetStatus())
	}
}

func TestEnvelope_HumaValidationBecomesValidationFailedWithFields(t *testing.T) {
	t.Parallel()
	env := FromValidation("req-2", []FieldError{{Field: "body.name", Message: "expected string"}})

	if env.ErrorBody.Code != "VALIDATION_FAILED" || env.GetStatus() != 400 {
		t.Fatalf("FromValidation() = %+v, want code VALIDATION_FAILED status 400", env.ErrorBody)
	}
	fields, ok := env.ErrorBody.Details["fields"].([]FieldError)
	if !ok || len(fields) != 1 || fields[0].Field != "body.name" {
		t.Fatalf("details.fields = %v, want one entry for body.name", env.ErrorBody.Details["fields"])
	}
}

func TestMap_PolicyErrorAndFallback(t *testing.T) {
	t.Parallel()

	tf := Map(&core.PolicyError{Code: core.TierForbidden}, "req-3")
	if tf.ErrorBody.Code != "TIER_FORBIDDEN" || tf.GetStatus() != 403 {
		t.Errorf("Map(PolicyError) = %+v, want TIER_FORBIDDEN/403", tf.ErrorBody)
	}

	unexpected := Map(context.DeadlineExceeded, "req-4")
	if unexpected.ErrorBody.Code != "INTERNAL" || unexpected.GetStatus() != 500 {
		t.Errorf("Map(unrecognised error) = %+v, want INTERNAL/500", unexpected.ErrorBody)
	}
}

func TestMap_UnmappedKindAndCodeFallBack(t *testing.T) {
	t.Parallel()

	// kafka.Kind and core.Code are plain ints, not compiler-closed enums, so
	// a value outside kafka.Kinds()/core.Codes() is reachable if the two
	// ever drift apart from their tables — Map must still degrade safely.
	kindFallback := Map(&kafka.Error{Kind: kafka.Kind(999)}, "req-6")
	if kindFallback.ErrorBody.Code != "KAFKA_ERROR" || kindFallback.GetStatus() != 502 {
		t.Errorf("Map(unmapped Kind) = %+v, want KAFKA_ERROR/502", kindFallback.ErrorBody)
	}

	codeFallback := Map(&core.PolicyError{Code: core.Code(999)}, "req-7")
	if codeFallback.ErrorBody.Code != "INTERNAL" || codeFallback.GetStatus() != 500 {
		t.Errorf("Map(unmapped Code) = %+v, want INTERNAL/500", codeFallback.ErrorBody)
	}
}

func TestEnvelope_Error(t *testing.T) {
	t.Parallel()
	env := FromInternal("req-8", context.DeadlineExceeded)
	if got := error(env).Error(); got != env.ErrorBody.Message {
		t.Errorf("Error() = %q, want %q", got, env.ErrorBody.Message)
	}
}

func TestUnauthenticated(t *testing.T) {
	t.Parallel()
	env := Unauthenticated("req-5")
	if env.ErrorBody.Code != "UNAUTHENTICATED" || env.GetStatus() != 401 || env.ErrorBody.RequestID != "req-5" {
		t.Errorf("Unauthenticated() = %+v", env.ErrorBody)
	}
}

func TestRequestIDContext(t *testing.T) {
	t.Parallel()
	if got := RequestIDFrom(context.Background()); got != "" {
		t.Errorf("RequestIDFrom(empty) = %q, want \"\"", got)
	}
	ctx := SetRequestID(context.Background(), "abc-123")
	if got := RequestIDFrom(ctx); got != "abc-123" {
		t.Errorf("RequestIDFrom() = %q, want abc-123", got)
	}
}
