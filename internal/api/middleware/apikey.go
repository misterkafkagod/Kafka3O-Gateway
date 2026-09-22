package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/misterkafkagod/kafka3o/internal/api/errors"
	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// RouteCommand identifies which catalog command a request's matched route
// maps to (TECH-SPEC O5) — just enough for APIKey to decide whether a 401
// deserves a REJECTED audit line (W commands only, FUNC-SPEC §8.5 scope)
// and, if so, which CommandID/CommandName to put on it.
type RouteCommand struct {
	CommandID   string
	CommandName string
	IsWrite     bool
}

// RouteLookup resolves a request to the RouteCommand its matched operation
// carries; ok is false for a request no registered operation matches.
type RouteLookup func(r *http.Request) (rc RouteCommand, ok bool)

// Key is one configured API key (TECH-SPEC B4): only its SHA-256 digest is
// ever held in memory. internal/api may not import internal/config
// (TECH-SPEC §5.3) — the composition root (Task 1.10) maps config.Key onto
// this type.
type Key struct {
	ID     string
	Tier   string
	SHA256 [32]byte
}

// breakGlassMaxBytes caps X-Break-Glass-Reason (TECH-SPEC C2).
const breakGlassMaxBytes = 512

// APIKey authenticates every request (FUNC-SPEC B3: no endpoint is exempt —
// including /health/*, /openapi.json, /docs). A presented key is SHA-256
// hashed and compared in constant time against every configured digest
// (TECH-SPEC B4); on success it builds a core.Caller and stores it in the
// request context. When authEnabled is false, every caller is an operator
// with a nil key id (FUNC-SPEC D3; TECH-SPEC C14) and no key is ever read.
//
// A CORS preflight (OPTIONS) request is passed straight through: browsers
// never attach X-Api-Key to a preflight, and a preflight never reaches an
// operation handler — the CORS middleware later in the chain answers it
// directly. This keeps the documented chain order (request-id → api-key →
// CORS → otelhttp) intact while still letting a real cross-origin request
// negotiate before it is authenticated.
// lookup and auditor may be nil in isolation (e.g. a test exercising only
// authentication itself): a nil auditor or lookup simply means a 401 is
// never audited, the same as an unmatched route.
func APIKey(keys []Key, authEnabled bool, lookup RouteLookup, auditor *audit.Auditor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}

			breakGlass := sanitizeBreakGlass(r.Header.Get(HeaderBreakGlass))

			if !authEnabled {
				next.ServeHTTP(w, r.WithContext(withCaller(r.Context(), core.Caller{
					Tier:             core.TierOperator,
					BreakGlassReason: breakGlass,
					RequestID:        errors.RequestIDFrom(r.Context()),
					ClientIP:         clientIPFrom(r.Context()),
					Client:           "http",
				})))
				return
			}

			key, ok := matchKey(r.Header.Get(HeaderAPIKey), keys)
			if !ok {
				auditUnauthenticatedRejection(r, lookup, auditor, breakGlass)
				writeUnauthenticated(w, r)
				return
			}

			id := key.ID
			next.ServeHTTP(w, r.WithContext(withCaller(r.Context(), core.Caller{
				KeyID:            &id,
				Tier:             key.Tier,
				BreakGlassReason: breakGlass,
				RequestID:        errors.RequestIDFrom(r.Context()),
				ClientIP:         clientIPFrom(r.Context()),
				Client:           "http",
			})))
		})
	}
}

// auditUnauthenticatedRejection emits a REJECTED RESULT audit event for a
// 401 on a W command's route (FUNC-SPEC §9.1 rules): R commands are never
// audited (FUNC-SPEC §8.5 scope), and a route lookup miss (an unknown path,
// or a non-catalog endpoint like /health) is silently skipped rather than
// guessed at. There is no valid Caller for a 401 (TECH-SPEC C14: no key was
// ever resolved), so this builds the event directly instead of going
// through core.CheckAudited, which needs one.
func auditUnauthenticatedRejection(r *http.Request, lookup RouteLookup, auditor *audit.Auditor, breakGlass string) {
	if lookup == nil || auditor == nil {
		return
	}
	rc, ok := lookup(r)
	if !ok || !rc.IsWrite {
		return
	}

	var bg *audit.BreakGlass
	if breakGlass != "" {
		bg = &audit.BreakGlass{Reason: breakGlass}
	}
	auditor.Result(r.Context(), audit.Event{
		EventID:     audit.NewEventID(),
		Timestamp:   time.Now(),
		RequestID:   errors.RequestIDFrom(r.Context()),
		CommandID:   rc.CommandID,
		CommandName: rc.CommandName,
		Target:      audit.Target{Type: "route", Name: r.URL.Path},
		Caller:      audit.Caller{ClientIP: clientIPFrom(r.Context())},
		BreakGlass:  bg,
		Outcome:     audit.OutcomeRejected,
		Severity:    audit.SeverityFor(rc.CommandID, audit.OutcomeRejected, bg != nil),
	})
}

// matchKey hashes presented and compares it in constant time against every
// configured digest, so response timing never reveals which prefix matched.
func matchKey(presented string, keys []Key) (Key, bool) {
	if presented == "" {
		return Key{}, false
	}
	digest := sha256.Sum256([]byte(presented))
	for _, k := range keys {
		if subtle.ConstantTimeCompare(digest[:], k.SHA256[:]) == 1 {
			return k, true
		}
	}
	return Key{}, false
}

func writeUnauthenticated(w http.ResponseWriter, r *http.Request) {
	env := errors.Unauthenticated(errors.RequestIDFrom(r.Context()))
	w.Header().Set(HeaderContentType, "application/json")
	w.WriteHeader(env.GetStatus())
	_ = json.NewEncoder(w).Encode(env)
}

// sanitizeBreakGlass strips control characters and caps the result at
// breakGlassMaxBytes on a rune boundary, so the result is always valid UTF-8
// (TECH-SPEC C2).
func sanitizeBreakGlass(raw string) string {
	if raw == "" {
		return ""
	}
	buf := make([]byte, 0, min(len(raw), breakGlassMaxBytes))
	for _, r := range raw {
		if r < 0x20 || r == 0x7f {
			continue
		}
		if len(buf)+utf8.RuneLen(r) > breakGlassMaxBytes {
			break
		}
		buf = utf8.AppendRune(buf, r)
	}
	return string(buf)
}
