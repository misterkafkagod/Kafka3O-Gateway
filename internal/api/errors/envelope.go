package errors

import (
	"context"
	"errors"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// Envelope is the FUNC-SPEC §8.3 error response shape:
// { error: { code, message, status, requestId, kafkaError?, details? } }.
// It implements huma.StatusError structurally (GetStatus() int, Error()
// string) without this package importing huma — internal/api/api.go is the
// only place that wires an Envelope into Huma's error model.
type Envelope struct {
	ErrorBody Body `json:"error"`
}

// Body is the envelope's inner object.
type Body struct {
	Code       string          `json:"code"`
	Message    string          `json:"message"`
	Status     int             `json:"status"`
	RequestID  string          `json:"requestId"`
	KafkaError *KafkaErrorInfo `json:"kafkaError,omitempty"`
	Details    map[string]any  `json:"details,omitempty"`
}

// KafkaErrorInfo preserves the broker's own code and name (FUNC-SPEC O5).
type KafkaErrorInfo struct {
	Code int16  `json:"code"`
	Name string `json:"name"`
}

// FieldError is one entry of details.fields[] for a validation failure.
type FieldError struct {
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

// Error implements error.
func (e *Envelope) Error() string { return e.ErrorBody.Message }

// GetStatus implements huma.StatusError.
func (e *Envelope) GetStatus() int { return e.ErrorBody.Status }

// Unauthenticated builds the 401 envelope (FUNC-SPEC §8.4): no Kafka error,
// no descriptor — the caller never got past node D of the request pipeline
// (FUNC-SPEC §9.1).
func Unauthenticated(requestID string) *Envelope {
	m := unauthenticatedMapping()
	return &Envelope{ErrorBody: Body{
		Code:      m.Code,
		Message:   "missing or unknown API key",
		Status:    m.Status,
		RequestID: requestID,
	}}
}

// Map builds the envelope for a service-layer error: a *kafka.Error maps
// through kindTable (preserving the broker's code and name), a
// *core.PolicyError maps through codeTable, and anything else falls back to
// 500 INTERNAL (FUNC-SPEC §8.4).
func Map(err error, requestID string) *Envelope {
	var ke *kafka.Error
	if errors.As(err, &ke) {
		m, ok := kindMapping(ke.Kind)
		if !ok {
			m = httpMapping{502, "KAFKA_ERROR"}
		}
		body := Body{Code: m.Code, Message: ke.Error(), Status: m.Status, RequestID: requestID}
		if ke.KafkaName != "" {
			body.KafkaError = &KafkaErrorInfo{Code: ke.KafkaCode, Name: ke.KafkaName}
		}
		if ke.Resource != "" {
			body.Details = map[string]any{"resource": ke.Resource}
		}
		return &Envelope{ErrorBody: body}
	}

	var pe *core.PolicyError
	if errors.As(err, &pe) {
		m, ok := codeMapping(pe.Code)
		if !ok {
			m = httpMapping{500, "INTERNAL"}
		}
		return &Envelope{ErrorBody: Body{
			Code: m.Code, Message: pe.Error(), Status: m.Status, RequestID: requestID,
			Details: pe.Details,
		}}
	}

	return FromInternal(requestID, err)
}

// FromValidation builds the 400 VALIDATION_FAILED envelope Huma's own
// request validation produces (FUNC-SPEC §8.3, §8.4).
func FromValidation(requestID string, fields []FieldError) *Envelope {
	details := map[string]any{}
	if len(fields) > 0 {
		details["fields"] = fields
	}
	return &Envelope{ErrorBody: Body{
		Code:      "VALIDATION_FAILED",
		Message:   "validation failed",
		Status:    400,
		RequestID: requestID,
		Details:   details,
	}}
}

// FromInternal builds the 500 INTERNAL envelope for an error Huma's own
// machinery raised that isn't a validation failure (FUNC-SPEC §8.4).
func FromInternal(requestID string, cause error) *Envelope {
	msg := "an unexpected error occurred"
	if cause != nil {
		msg = cause.Error()
	}
	return &Envelope{ErrorBody: Body{
		Code:      "INTERNAL",
		Message:   msg,
		Status:    500,
		RequestID: requestID,
	}}
}

// requestIDKey is the context key the request-id middleware sets and every
// error constructor reads (TECH-SPEC C2, FUNC-SPEC X4).
type requestIDKey struct{}

// SetRequestID returns a context carrying id.
func SetRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom returns the request id stored in ctx, or "" if none.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
