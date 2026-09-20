// Package errors is the gateway's single error-envelope home (TECH-SPEC S5):
// the FUNC-SPEC §8.4 taxonomy as two lookup tables, the §8.3 error envelope
// type, and the request-id context accessor every other api subpackage that
// needs to report an error (middleware, health, and later services) shares.
// The package is huma-free by design: internal/api/api.go is the only place
// that wires it into Huma's error model, keeping this package reusable by
// any transport.
package errors

import (
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// httpMapping is one FUNC-SPEC §8.4 row: the HTTP status and wire code a
// kafka.Kind or core.Code maps to.
type httpMapping struct {
	Status int
	Code   string
}

// kindTable returns every kafka.Kind's FUNC-SPEC §8.4 row. It is a function,
// not a package-level var (TECH-SPEC §2.3, matching internal/command.Table),
// so the table is never shared mutable state.
func kindTable() map[kafka.Kind]httpMapping {
	return map[kafka.Kind]httpMapping{
		kafka.KindNotFound:               {404, "NOT_FOUND"},
		kafka.KindAlreadyExists:          {409, "ALREADY_EXISTS"},
		kafka.KindGroupActive:            {409, "GROUP_ACTIVE"},
		kafka.KindReassignmentInProgress: {409, "REASSIGNMENT_IN_PROGRESS"},
		kafka.KindTimeout:                {504, "KAFKA_TIMEOUT"},
		kafka.KindUnavailable:            {503, "CLUSTER_UNAVAILABLE"},
		// KindUnsupported surfaces as KAFKA_ERROR (TECH-SPEC C3): the cluster
		// lacks the feature; kafkaError.name carries UNSUPPORTED_VERSION.
		kafka.KindUnsupported: {502, "KAFKA_ERROR"},
		kafka.KindBroker:      {502, "KAFKA_ERROR"},
	}
}

// codeTable returns every core.Code's FUNC-SPEC §8.4 row.
func codeTable() map[core.Code]httpMapping {
	return map[core.Code]httpMapping{
		core.TierForbidden:        {403, "TIER_FORBIDDEN"},
		core.ReadOnlyMode:         {403, "READ_ONLY_MODE"},
		core.OperationDisabled:    {403, "OPERATION_DISABLED"},
		core.DataPlaneLocked:      {403, "DATA_PLANE_LOCKED"},
		core.ConfirmationMismatch: {400, "CONFIRMATION_MISMATCH"},
		core.BoundExceeded:        {400, "BOUND_EXCEEDED"},
		core.Validation:           {400, "VALIDATION_FAILED"},
	}
}

// kindMapping returns the row for k, and whether one exists.
func kindMapping(k kafka.Kind) (httpMapping, bool) {
	m, ok := kindTable()[k]
	return m, ok
}

// codeMapping returns the row for c, and whether one exists.
func codeMapping(c core.Code) (httpMapping, bool) {
	m, ok := codeTable()[c]
	return m, ok
}

// unauthenticatedMapping is FUNC-SPEC §8.4's 401 row. It has no Kind or Code
// of its own — the api-key middleware (Task 1.8.3) constructs it directly,
// before any command descriptor or Kafka call is in play.
func unauthenticatedMapping() httpMapping {
	return httpMapping{401, "UNAUTHENTICATED"}
}
