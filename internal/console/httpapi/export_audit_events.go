package httpapi

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
)

// MaxAuditExportRows bounds one export; narrower filters export more history.
const MaxAuditExportRows = 10000

// exportAuditEvents returns the authorized scope as a CSV attachment and audits the export.
func (a *SessionAPI) exportAuditEvents(w http.ResponseWriter, r *http.Request) {
	query, err := readAuditQuery(r, 1000, 1000)
	if err != nil {
		a.writeRequestError(w, err)
		return
	}
	if _, ok := a.authorizeAuditQuery(w, r, query, "console.audit.export"); !ok {
		return
	}
	filter := query.filter
	filter.Limit = 1000
	records := []AuditRecord{}
	for len(records) < MaxAuditExportRows {
		page, err := a.audit.ListAudit(r.Context(), filter)
		if err != nil {
			a.logger.Error("Console audit export unavailable", "error_type", fmt.Sprintf("%T", err))
			a.writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable", "Audit is unavailable")
			return
		}
		records = append(records, page...)
		if len(page) < filter.Limit {
			break
		}
		filter.AfterSequence = page[len(page)-1].Sequence
	}
	if len(records) > MaxAuditExportRows {
		records = records[:MaxAuditExportRows]
	}
	var body bytes.Buffer
	if err := writeAuditCSV(&body, records); err != nil {
		a.writeProblem(w, http.StatusServiceUnavailable, "audit_unavailable", "Audit is unavailable")
		return
	}
	scope := "global"
	if !query.filter.Global {
		scope = query.filter.ClusterID
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="kafka3o-audit-%s-%s.csv"`, scope, time.Now().UTC().Format("20060102T150405Z")))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body.Bytes())
}
