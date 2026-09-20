// Package api is the gateway's HTTP composition root (TECH-SPEC §5.1,
// §6.2): it wires the middleware chain, Huma's error model, and every
// operation onto a net/http mux. It takes only plain values as
// dependencies (Deps) — internal/api may not import internal/config
// (TECH-SPEC §5.3) — so the process composition root (Task 1.10) is the
// only place that maps config.Config onto Deps.
package api

import (
	"errors"
	"net"
	"net/http"
	"sync"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"

	apierrors "github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/api/health"
	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
)

// basePath is every operation's URL prefix (TECH-SPEC §6.2). It is not
// applied to /openapi.json or /docs, which stay unprefixed.
const basePath = "/v1"

// Deps are the gateway's start-up dependencies (TECH-SPEC §5.1).
type Deps struct {
	// Cluster is the Kafka admin port readiness probes against (C3).
	Cluster health.ClusterProbe
	// AuditStatus reports the configured audit sink's health (TECH-SPEC B5).
	AuditStatus health.AuditStatus
	// Keys are the configured API keys (TECH-SPEC B4). Ignored when
	// AuthEnabled is false.
	Keys []middleware.Key
	// AuthEnabled toggles API-key enforcement (FUNC-SPEC D3).
	AuthEnabled bool
	// CORSOrigins are the browser origins allowed to call the gateway
	// (FUNC-SPEC X5).
	CORSOrigins []string
	// TrustedProxies resolve X-Forwarded-For (TECH-SPEC §6.1 B6).
	TrustedProxies []*net.IPNet
	// DocsEnabled toggles the built-in /docs renderer.
	DocsEnabled bool
}

// New builds the gateway's HTTP handler: Huma operations on a net/http mux
// wrapped by the middleware chain, in the documented order (TECH-SPEC
// §6.2) request-id → client-ip → api-key → CORS → mux.
func New(deps Deps) http.Handler {
	mux := http.NewServeMux()

	config := huma.DefaultConfig("Kafka Application Gateway", "1.0.0")
	if !deps.DocsEnabled {
		config.DocsPath = ""
	}
	installErrorModel()

	humaAPI := humago.New(mux, config)
	registerHealth(humaAPI, deps)

	var handler http.Handler = mux
	handler = middleware.CORS(deps.CORSOrigins, deps.AuthEnabled)(handler)
	handler = middleware.APIKey(deps.Keys, deps.AuthEnabled)(handler)
	handler = middleware.ClientIP(deps.TrustedProxies)(handler)
	handler = middleware.RequestID(handler)
	return handler
}

// registerHealth wires the two C3 operations (FUNC-SPEC §8.7 C3, O6).
func registerHealth(humaAPI huma.API, deps Deps) {
	huma.Register(humaAPI, huma.Operation{
		OperationID: "health-live",
		Method:      http.MethodGet,
		Path:        basePath + "/health/live",
		Summary:     "Liveness probe",
		Tags:        []string{"Health"},
		Extensions:  commandExtension("C3"),
	}, health.Live)

	huma.Register(humaAPI, huma.Operation{
		OperationID: "health-ready",
		Method:      http.MethodGet,
		Path:        basePath + "/health/ready",
		Summary:     "Readiness probe",
		Tags:        []string{"Health"},
		Extensions:  commandExtension("C3"),
	}, health.Ready(deps.Cluster, deps.AuditStatus))
}

// installErrorModelOnce guards huma.NewError/huma.NewErrorWithContext:
// they are Huma's own package-level vars, so every New() call — including
// concurrent ones from parallel component tests — must assign them at
// most once instead of racing to overwrite each other. This is a
// synchronization primitive for an upstream library's global, not gateway
// domain state (D2).
//
//nolint:gochecknoglobals // guards a one-time install of Huma's own package vars; not domain state (D2)
var installErrorModelOnce sync.Once

// installErrorModel replaces Huma's built-in error model with the
// FUNC-SPEC §8.3 envelope (TECH-SPEC O4). This is the only place either
// override is set — internal/api/errors itself stays huma-free.
func installErrorModel() {
	installErrorModelOnce.Do(func() {
		huma.NewError = func(status int, msg string, errs ...error) huma.StatusError {
			return apierrors.FromInternal("", combineErrs(msg, errs))
		}

		huma.NewErrorWithContext = func(ctx huma.Context, status int, msg string, errs ...error) huma.StatusError {
			requestID := apierrors.RequestIDFrom(ctx.Context())
			if msg == "validation failed" {
				return apierrors.FromValidation(requestID, validationFields(errs))
			}
			return apierrors.FromInternal(requestID, combineErrs(msg, errs))
		}
	})
}

// validationFields converts Huma's own request-validation errors into the
// envelope's details.fields[] shape (FUNC-SPEC §8.4).
func validationFields(errs []error) []apierrors.FieldError {
	fields := make([]apierrors.FieldError, 0, len(errs))
	for _, e := range errs {
		if e == nil {
			continue
		}
		var detailer huma.ErrorDetailer
		if errors.As(e, &detailer) {
			d := detailer.ErrorDetail()
			fields = append(fields, apierrors.FieldError{Field: d.Location, Message: d.Message})
			continue
		}
		fields = append(fields, apierrors.FieldError{Message: e.Error()})
	}
	return fields
}

// combineErrs folds Huma's message and wrapped errors into one cause for
// FromInternal, which only takes a single error.
func combineErrs(msg string, errs []error) error {
	if len(errs) == 0 {
		return errors.New(msg)
	}
	return errs[0]
}
