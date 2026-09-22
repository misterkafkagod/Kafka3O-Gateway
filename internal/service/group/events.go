package group

import (
	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/command"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// newEvent builds the base audit.Event for one G4-G7 command invocation
// (FUNC-SPEC §8.5): its ATTEMPT and RESULT (or, for a gate rejection or a
// dry-run, its one RESULT) share this EventID and Target/Caller/BreakGlass.
// commandID must name a G4-G7 command.Table entry; groupID is the group
// being acted on — for G7 that is the target group, never the source
// (FUNC-SPEC §8.6: confirm and the audit target are both the target).
func (s *Service) newEvent(caller core.Caller, commandID, groupID string) audit.Event {
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

	return audit.Event{
		EventID:     s.newEventID(),
		Timestamp:   s.now(),
		RequestID:   caller.RequestID,
		CommandID:   desc.ID,
		CommandName: desc.Name,
		Target:      audit.Target{Type: "group", Name: groupID},
		Caller:      audit.Caller{KeyID: keyID, Tier: caller.Tier, ClientIP: caller.ClientIP},
		BreakGlass:  breakGlass,
	}
}
