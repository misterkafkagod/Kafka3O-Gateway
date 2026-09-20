// Package core holds the transport-neutral pieces every service uses
// (TECH-SPEC §2.0 P1): who is calling (Caller), what is configured
// (Policy), the shared error vocabulary (PolicyError), and the generic gate
// check every service method runs through (run). Package core never imports
// internal/service/gates — see run.go for why.
package core

// Tier values a Caller may carry (FUNC-SPEC F1). Defined here rather than
// imported from internal/config: internal/service may not depend on
// internal/config (TECH-SPEC §5.3) — the HTTP layer (Task 1.8) maps the
// loaded key tiers onto these same string values when it builds a Caller.
const (
	TierReader   = "reader"
	TierOperator = "operator"
)

// Caller identifies who is making a request (FUNC-SPEC §8.5 caller fields).
// KeyID is nil when authentication is disabled or the request was rejected
// before a key could be resolved (TECH-SPEC C14).
type Caller struct {
	KeyID *string
	Tier  string
	// BreakGlassReason is non-empty only when the caller supplied
	// X-Break-Glass-Reason (FUNC-SPEC §8.2); its presence is exactly what
	// node K2 of the request pipeline checks (FUNC-SPEC §9.1).
	BreakGlassReason string
	RequestID        string
	ClientIP         string
	// Client identifies the calling surface for audit provenance
	// (FUNC-SPEC §8.5), e.g. "http".
	Client string
}
