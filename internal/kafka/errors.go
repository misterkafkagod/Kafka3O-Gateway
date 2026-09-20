package kafka

import (
	"errors"
	"fmt"
)

// Kind classifies a port failure. Adapters map broker errors onto exactly
// these values so every adapter reports the same Kind for the same condition
// (TECH-SPEC L2); the API layer maps each Kind to one FUNC-SPEC §8.4 row.
type Kind int

// Kind values.
const (
	// KindBroker is any broker error not classified below → 502 KAFKA_ERROR.
	KindBroker Kind = iota
	// KindNotFound: unknown topic, partition, offset, group, broker, or user → 404.
	KindNotFound
	// KindAlreadyExists: topic or user already exists → 409 ALREADY_EXISTS.
	KindAlreadyExists
	// KindGroupActive: the group still has live members → 409 GROUP_ACTIVE.
	KindGroupActive
	// KindReassignmentInProgress: a conflicting reassignment is running → 409.
	KindReassignmentInProgress
	// KindTimeout: the operation or its context deadline expired → 504 KAFKA_TIMEOUT.
	KindTimeout
	// KindUnavailable: no broker reachable / metadata unavailable → 503 CLUSTER_UNAVAILABLE.
	KindUnavailable
	// KindUnsupported: the cluster lacks the feature (UNSUPPORTED_VERSION) → 502 (TECH-SPEC C3).
	KindUnsupported
)

// String returns the Kind's name.
func (k Kind) String() string {
	switch k {
	case KindBroker:
		return "broker"
	case KindNotFound:
		return "not_found"
	case KindAlreadyExists:
		return "already_exists"
	case KindGroupActive:
		return "group_active"
	case KindReassignmentInProgress:
		return "reassignment_in_progress"
	case KindTimeout:
		return "timeout"
	case KindUnavailable:
		return "unavailable"
	case KindUnsupported:
		return "unsupported"
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

// Kinds lists every Kind, for exhaustiveness tests over the error tables (TECH-SPEC O4).
func Kinds() []Kind {
	return []Kind{
		KindBroker, KindNotFound, KindAlreadyExists, KindGroupActive,
		KindReassignmentInProgress, KindTimeout, KindUnavailable, KindUnsupported,
	}
}

// Error is the only error type that crosses the port (TECH-SPEC L2). It keeps
// the broker's own code and name so the API envelope can preserve them
// (FUNC-SPEC O5, §8.3 kafkaError).
type Error struct {
	Kind Kind
	// Resource names what was not found or conflicted, e.g. "topic", "group".
	Resource string
	// KafkaCode and KafkaName are the broker error code and name when a broker
	// produced the failure; zero and empty for gateway-side conditions.
	KafkaCode int16
	KafkaName string
	// Cause is the underlying adapter error, for logs only.
	Cause error
}

// Error implements error.
func (e *Error) Error() string {
	msg := "kafka: " + e.Kind.String()
	if e.Resource != "" {
		msg += " (" + e.Resource + ")"
	}
	if e.KafkaName != "" {
		msg += fmt.Sprintf(" [%s/%d]", e.KafkaName, e.KafkaCode)
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

// Unwrap exposes the cause to errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.Cause }

// KindOf returns the Kind of err when it is (or wraps) a port Error.
func KindOf(err error) (Kind, bool) {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Kind, true
	}
	return 0, false
}

// IsKind reports whether err is (or wraps) a port Error of the given Kind.
func IsKind(err error, k Kind) bool {
	got, ok := KindOf(err)
	return ok && got == k
}
