package api

import "github.com/misterkafkagod/kafka3o/internal/command"

// commandIDExtension is the x-command-id key at TECH-SPEC O5 (FUNC-SPEC
// §9.7 O1): every registered Huma operation carries exactly one, set to a
// command.Table id.
const commandIDExtension = "x-command-id"

// commandExtension builds a Huma operation's Extensions map for id.
func commandExtension(id string) map[string]any {
	return map[string]any{commandIDExtension: id}
}

// implementedCommandIDs are every command.Table id this phase actually
// wires an HTTP operation for. It grows as later tasks register more
// operations (TECH-SPEC O5); it is a function, not a package-level var
// (matching internal/command.Table), so it is never shared mutable state.
func implementedCommandIDs() map[string]bool {
	return map[string]bool{
		"C3":  true,
		"C1":  true,
		"C2":  true,
		"C4":  true,
		"T1":  true,
		"T2":  true,
		"T3":  true,
		"T4":  true,
		"M1":  true,
		"M2":  true,
		"M3":  true,
		"M4":  true,
		"M5":  true,
		"M6":  true,
		"M7":  true,
		"G1":  true,
		"G2":  true,
		"G3":  true,
		"T5":  true,
		"T6":  true,
		"T7":  true,
		"T8":  true,
		"T9":  true,
		"T10": true,
		"T11": true,
		"T12": true,
	}
}

// Pending returns every command.Table id not yet wired to an HTTP
// operation (FUNC-SPEC §9.7 O1): the catalog minus implementedCommandIDs.
func Pending() []string {
	implemented := implementedCommandIDs()
	tbl := command.Table()
	out := make([]string, 0, len(tbl))
	for _, d := range tbl {
		if !implemented[d.ID] {
			out = append(out, d.ID)
		}
	}
	return out
}
