package httpapi

import (
	"encoding/csv"
	"io"
	"strconv"
	"time"
)

// writeAuditCSV writes sanitized audit records with a header row; every text cell is formula-neutralized.
func writeAuditCSV(w io.Writer, records []AuditRecord) error {
	out := csv.NewWriter(w)
	if err := out.Write([]string{"sequence", "occurred_at", "operation_id", "actor_id", "source", "cluster_id", "environment", "action",
		"resource_kind", "resource_id", "decision", "outcome", "reason", "before_state", "after_state", "policy_revision"}); err != nil {
		return err
	}
	for _, r := range records {
		row := []string{strconv.FormatInt(r.Sequence, 10), r.OccurredAt.UTC().Format(time.RFC3339Nano)}
		for _, value := range []string{r.OperationID, r.ActorID, r.Source, r.ClusterID, r.Environment, r.Action, r.ResourceKind, r.ResourceID,
			r.Decision, r.Outcome, r.Reason, r.BeforeState, r.AfterState} {
			row = append(row, neutralizeCSVCell(value))
		}
		if err := out.Write(append(row, strconv.FormatInt(r.PolicyRevision, 10))); err != nil {
			return err
		}
	}
	out.Flush()
	return out.Error()
}
