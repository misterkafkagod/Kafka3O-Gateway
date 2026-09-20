package core

// Policy holds the configuration gates.Check and the HTTP layer's auth
// middleware evaluate (FUNC-SPEC §8.2 switches: F2, F3, F6, plus whether
// authentication is required at all). It is a plain value type, not
// internal/config.Policy: internal/service may not import internal/config
// (TECH-SPEC §5.3) — the composition root (Task 1.10) maps the loaded
// configuration onto this type.
type Policy struct {
	AuthEnabled   bool
	ReadOnly      bool
	DataPlaneLock bool
	// Disabled lists the per-operation switches (FUNC-SPEC F3): Disabled[id]
	// is true when that catalog command id is administratively turned off.
	Disabled map[string]bool
}
