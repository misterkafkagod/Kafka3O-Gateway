// Package command is the declarative command table (TECH-SPEC §2.3): one
// Descriptor per FUNC-SPEC §5 catalog entry. Gates, audit, and the OpenAPI
// bijection test key off these fields — never off command IDs (TECH-SPEC O1).
package command

// Access is the key tier a command requires (FUNC-SPEC §5.4 legend).
type Access string

// Access values: R is reachable by reader keys, W by operator keys only.
const (
	R Access = "R"
	W Access = "W"
)

// Descriptor describes one catalog command.
type Descriptor struct {
	// ID is the catalog identifier, e.g. "T7".
	ID string
	// Name is the human-readable command name from the catalog.
	Name string
	// Access is the required key tier (FUNC-SPEC F1).
	Access Access
	// Destructive marks the FUNC-SPEC §5.6 set: F3 switches and F4
	// confirmation / dry-run apply. For S1, S2, C12 the flag covers the
	// command; the service layer applies Plan/Apply to the mutating sub-operations.
	Destructive bool
	// DataPlane marks the message commands M1–M8 blocked by the F6 lock.
	DataPlane bool
}

// Table returns the 41 catalog descriptors in FUNC-SPEC §5 order.
// It returns a fresh slice on every call so callers cannot mutate the table.
func Table() []Descriptor {
	return []Descriptor{
		// §5.1 P1 — core inspection & data plane
		{ID: "C1", Name: "Describe cluster", Access: R},
		{ID: "C2", Name: "Describe broker configuration", Access: R},
		{ID: "C3", Name: "Gateway health & connectivity", Access: R},
		{ID: "C4", Name: "Cluster health summary", Access: R},
		{ID: "T1", Name: "List topics", Access: R},
		{ID: "T2", Name: "Describe topic", Access: R},
		{ID: "T3", Name: "Partition / topic on-disk size", Access: R},
		{ID: "T4", Name: "Message count in a time window", Access: R},
		{ID: "M1", Name: "Read messages", Access: R, DataPlane: true},
		{ID: "M2", Name: "Fetch single message", Access: R, DataPlane: true},
		{ID: "M3", Name: "Search messages by regex", Access: R, DataPlane: true},
		{ID: "M4", Name: "Search messages by JSONPath filter", Access: R, DataPlane: true},
		{ID: "M5", Name: "Produce message(s)", Access: W, DataPlane: true},
		{ID: "M6", Name: "Bulk produce from upload", Access: W, DataPlane: true},
		{ID: "M7", Name: "Send tombstone for a key", Access: W, DataPlane: true},
		{ID: "G1", Name: "List consumer groups", Access: R},
		{ID: "G2", Name: "Describe consumer group", Access: R},
		{ID: "G3", Name: "Groups consuming a topic", Access: R},

		// §5.2 P2 — administration & destructive
		{ID: "T5", Name: "Create topic", Access: W},
		{ID: "T6", Name: "Bulk create topics", Access: W},
		{ID: "T7", Name: "Delete topic", Access: W, Destructive: true},
		{ID: "T8", Name: "Bulk delete topics", Access: W, Destructive: true},
		{ID: "T9", Name: "Alter topic configuration", Access: W, Destructive: true},
		{ID: "T10", Name: "Add partitions", Access: W, Destructive: true},
		{ID: "T11", Name: "Delete records", Access: W, Destructive: true},
		{ID: "T12", Name: "Purge topic", Access: W, Destructive: true},
		{ID: "G4", Name: "Reset consumer group offsets", Access: W, Destructive: true},
		{ID: "G5", Name: "Delete consumer group", Access: W, Destructive: true},
		{ID: "G6", Name: "Remove members from consumer group", Access: W, Destructive: true},
		{ID: "G7", Name: "Clone consumer group offsets", Access: W, Destructive: true},
		{ID: "M8", Name: "Replay / copy message range", Access: W, Destructive: true, DataPlane: true},
		{ID: "C5", Name: "Alter broker dynamic configuration", Access: W, Destructive: true},

		// §5.3 P3 — advanced cluster operations
		{ID: "C6", Name: "KRaft quorum status", Access: R},
		{ID: "C7", Name: "In-progress partition reassignments", Access: R},
		{ID: "C8", Name: "Broker disk usage per log directory", Access: R},
		{ID: "C9", Name: "Partition reassignment & leader election", Access: W, Destructive: true},
		{ID: "C10", Name: "Throughput sample", Access: R},
		{ID: "C11", Name: "Export topic / cluster definitions", Access: R},
		{ID: "C12", Name: "Import / apply topic definitions", Access: W, Destructive: true},
		{ID: "S1", Name: "Manage SCRAM credentials", Access: W, Destructive: true},
		{ID: "S2", Name: "Manage client quotas", Access: W, Destructive: true},
	}
}

// Lookup returns the descriptor for a catalog ID.
func Lookup(id string) (Descriptor, bool) {
	for _, d := range Table() {
		if d.ID == id {
			return d, true
		}
	}
	return Descriptor{}, false
}
