package cluster

import (
	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// newEvent builds the base audit.Event for one C5, C9, or C12 command
// invocation (FUNC-SPEC §8.5): its ATTEMPT and RESULT (or, for a gate
// rejection or a dry-run, its one RESULT) share this EventID and Target/
// Caller/BreakGlass. commandID must name a C5/C9/C12 command.Table entry;
// target is the broker id (C5), the empty string (C9: no single target —
// FUNC-SPEC §8.6 has no confirm-target row naming one topic-partition), or
// the empty string (C12: the desired topic set, not any one topic).
func (s *Service) newEvent(caller core.Caller, commandID, target string) audit.Event {
	desc, _ := command.Lookup(commandID)

	var keyID *string
	if caller.KeyID != nil {
		id := *caller.KeyID
		keyID = &id
	}
	var breakGlass *audit.BreakGlass
	if caller.BreakGlassReason != "" {
		breakGlass = &audit.BreakGlass{Reason: caller.BreakGlassReason}
	}

	targetType := "cluster"
	if commandID == "C5" {
		targetType = "broker"
	}

	return audit.Event{
		EventID:     s.newEventID(),
		Timestamp:   s.now(),
		RequestID:   caller.RequestID,
		CommandID:   desc.ID,
		CommandName: desc.Name,
		Target:      audit.Target{Type: targetType, Name: target},
		Caller:      audit.Caller{KeyID: keyID, Tier: caller.Tier, ClientIP: caller.ClientIP},
		BreakGlass:  breakGlass,
	}
}
