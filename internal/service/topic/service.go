// Package topic implements the topic commands (FUNC-SPEC §8.7 T1-T6, T9,
// T10). Service holds only kafka.Admin (TECH-SPEC I2).
package topic

import (
	"context"
	"regexp"
	"sort"
	"time"

	"github.com/misterkafkagod/kafka3o/internal/audit"
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/core"
)

// Service implements the topic commands over a kafka.Admin.
type Service struct {
	admin kafka.Admin
	// auditor records T5, T6, T9, T10's ATTEMPT/RESULT audit trail
	// (FUNC-SPEC §8.5) — T1-T4 are reads and stay unaudited (FUNC-SPEC §8.5
	// scope).
	auditor    *audit.Auditor
	newEventID func() string
	now        func() time.Time
	// runner gates T5, T6, T9, T10 (FUNC-SPEC §9.1 nodes F-K3): an
	// operator-only caller, plus (for T9, T10, both in FUNC-SPEC §5.6) F3's
	// per-operation switch.
	runner core.Runner
}

// New builds a Service over admin, auditor, and runner.
func New(admin kafka.Admin, auditor *audit.Auditor, runner core.Runner) *Service {
	return &Service{admin: admin, auditor: auditor, newEventID: audit.NewEventID, now: time.Now, runner: runner}
}

// ListItem is one topic in a List result (FUNC-SPEC §8.7 T1).
type ListItem struct {
	Name              string
	Internal          bool
	PartitionCount    int
	ReplicationFactor int
}

// ListParams are List's inputs. Pattern is an RE2 regular expression matched
// unanchored against the full topic name; empty matches every topic
// (FUNC-SPEC §8.2 `?pattern=`; TECH-SPEC B7). Page is 1-based; callers own
// applying the `?page`/`?pageSize` defaults and ceiling (FUNC-SPEC §8.2).
type ListParams struct {
	Pattern         string
	IncludeInternal bool
	Page            int
	PageSize        int
}

// ListPage is List's pagination result (FUNC-SPEC §8.2 `?page`/`?pageSize`).
type ListPage struct {
	Page     int
	PageSize int
	Total    int
}

// List returns topics matching params.Pattern, sorted by name for stable
// paging, filtered by params.IncludeInternal, and paginated (FUNC-SPEC §8.7
// T1). An invalid Pattern reports *core.PolicyError{Code: core.InvalidRegex}
// (TECH-SPEC B7) rather than reaching the port.
func (s *Service) List(ctx context.Context, params ListParams) ([]ListItem, ListPage, error) {
	var pattern *regexp.Regexp
	if params.Pattern != "" {
		re, err := regexp.Compile(params.Pattern)
		if err != nil {
			return nil, ListPage{}, &core.PolicyError{Code: core.InvalidRegex, Message: err.Error()}
		}
		pattern = re
	}

	topics, err := s.admin.ListTopics(ctx)
	if err != nil {
		return nil, ListPage{}, err
	}

	matched := make([]kafka.TopicSummary, 0, len(topics))
	for _, t := range topics {
		if !params.IncludeInternal && t.Internal {
			continue
		}
		if pattern != nil && !pattern.MatchString(t.Name) {
			continue
		}
		matched = append(matched, t)
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })

	total := len(matched)
	page := ListPage{Page: params.Page, PageSize: params.PageSize, Total: total}

	start := (params.Page - 1) * params.PageSize
	if start < 0 || start >= total {
		return []ListItem{}, page, nil
	}
	end := min(start+params.PageSize, total)

	items := make([]ListItem, end-start)
	for i, t := range matched[start:end] {
		items[i] = ListItem{
			Name:              t.Name,
			Internal:          t.Internal,
			PartitionCount:    t.PartitionCount,
			ReplicationFactor: t.ReplicationFactor,
		}
	}
	return items, page, nil
}
