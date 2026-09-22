package core

import (
	"errors"
	"fmt"
)

// Code enumerates the policy-gate and validation failures a service or gate
// can report (FUNC-SPEC §8.4). Each maps to exactly one row of that table;
// the API layer (Task 1.8) owns the Code -> HTTP status mapping. Not every
// Code is producible yet: gates.Check (Task 1.7) only ever returns
// TierForbidden, ReadOnlyMode, OperationDisabled, or DataPlaneLocked;
// ConfirmationMismatch and BoundExceeded are used by later tasks (destructive
// Plan/Apply, bounded reads) that share this vocabulary. Validation and
// InvalidRegex are produced starting with Task 2.3 (topic.Service.List's
// `?pattern=` compile failure); InvalidJSONPath starting with Task 4.2
// (message.Service.Filter's `path` compile failure).
type Code int

// Code values (FUNC-SPEC §8.4).
const (
	TierForbidden Code = iota
	ReadOnlyMode
	OperationDisabled
	DataPlaneLocked
	ConfirmationMismatch
	BoundExceeded
	Validation
	InvalidRegex
	InvalidJSONPath
	BulkValidationFailed
	AuditUnavailable
	PayloadTooLarge
)

// String returns the Code's FUNC-SPEC §8.4 wire name.
func (c Code) String() string {
	switch c {
	case TierForbidden:
		return "TIER_FORBIDDEN"
	case ReadOnlyMode:
		return "READ_ONLY_MODE"
	case OperationDisabled:
		return "OPERATION_DISABLED"
	case DataPlaneLocked:
		return "DATA_PLANE_LOCKED"
	case ConfirmationMismatch:
		return "CONFIRMATION_MISMATCH"
	case BoundExceeded:
		return "BOUND_EXCEEDED"
	case Validation:
		return "VALIDATION_FAILED"
	case InvalidRegex:
		return "INVALID_REGEX"
	case InvalidJSONPath:
		return "INVALID_JSONPATH"
	case BulkValidationFailed:
		return "BULK_VALIDATION_FAILED"
	case AuditUnavailable:
		return "AUDIT_UNAVAILABLE"
	case PayloadTooLarge:
		return "PAYLOAD_TOO_LARGE"
	}
	return fmt.Sprintf("code(%d)", int(c))
}

// Codes lists every Code, for exhaustiveness tests over the error tables
// (TECH-SPEC O4).
func Codes() []Code {
	return []Code{
		TierForbidden, ReadOnlyMode, OperationDisabled, DataPlaneLocked,
		ConfirmationMismatch, BoundExceeded, Validation, InvalidRegex, InvalidJSONPath,
		BulkValidationFailed, AuditUnavailable, PayloadTooLarge,
	}
}

// PolicyError is a gate or validation failure (FUNC-SPEC §8.4). Message, when
// set, is additional detail for the caller (e.g. which field failed
// validation); it is never required. Details, when set, is rendered
// verbatim as the error envelope's details object (FUNC-SPEC §8.4
// BULK_VALIDATION_FAILED's `details.items[]`).
type PolicyError struct {
	Code    Code
	Message string
	Details map[string]any
}

// Error implements error.
func (e *PolicyError) Error() string {
	if e.Message == "" {
		return "policy: " + e.Code.String()
	}
	return "policy: " + e.Code.String() + ": " + e.Message
}

// CodeOf returns the Code of err when it is (or wraps) a *PolicyError.
func CodeOf(err error) (Code, bool) {
	var pe *PolicyError
	if errors.As(err, &pe) {
		return pe.Code, true
	}
	return 0, false
}

// IsCode reports whether err is (or wraps) a *PolicyError of the given Code.
func IsCode(err error, c Code) bool {
	got, ok := CodeOf(err)
	return ok && got == c
}
