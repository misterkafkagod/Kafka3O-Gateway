// Package group implements the consumer-group commands (FUNC-SPEC §8.7
// G1-G3). Service holds only kafka.Admin (TECH-SPEC I2, S2).
package group

import (
	"context"
	"sort"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// Service implements the group commands over a kafka.Admin.
type Service struct {
	admin kafka.Admin
}

// New builds a Service over admin.
func New(admin kafka.Admin) *Service {
	return &Service{admin: admin}
}

// ListItem is one group in a List result (FUNC-SPEC §8.7 G1).
type ListItem struct {
	ID           string
	State        string
	ProtocolType string
	MemberCount  int
}

// ListParams are List's inputs. State empty matches every state. Page is
// 1-based; callers own applying the `?page`/`?pageSize` defaults and
// ceiling (FUNC-SPEC §8.2), the same as topic.Service.List.
type ListParams struct {
	State    string
	Page     int
	PageSize int
}

// ListPage is List's pagination result (FUNC-SPEC §8.2 `?page`/`?pageSize`).
type ListPage struct {
	Page     int
	PageSize int
	Total    int
}

// List returns groups matching params.State, sorted by id for stable
// paging, and paginated (FUNC-SPEC §8.7 G1).
func (s *Service) List(ctx context.Context, params ListParams) ([]ListItem, ListPage, error) {
	var states []string
	if params.State != "" {
		states = []string{params.State}
	}

	groups, err := s.admin.ListGroups(ctx, states...)
	if err != nil {
		return nil, ListPage{}, err
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })

	total := len(groups)
	page := ListPage{Page: params.Page, PageSize: params.PageSize, Total: total}

	start := (params.Page - 1) * params.PageSize
	if start < 0 || start >= total {
		return []ListItem{}, page, nil
	}
	end := min(start+params.PageSize, total)

	items := make([]ListItem, end-start)
	for i, g := range groups[start:end] {
		items[i] = ListItem{ID: g.ID, State: g.State, ProtocolType: g.ProtocolType, MemberCount: g.MemberCount}
	}
	return items, page, nil
}

// MemberDetail is one live member of a group and its assignment
// (FUNC-SPEC §8.7 G2).
type MemberDetail struct {
	MemberID    string
	ClientID    string
	Host        string
	Assignments []kafka.TopicPartition
}

// OffsetDetail is one partition's committed offset, end offset, and lag
// (FUNC-SPEC §8.7 G2, G3). Lag is never negative: a committed offset ahead
// of the current end snapshot (a race between the two independent reads
// this method makes) reports zero, not a negative lag.
type OffsetDetail struct {
	Topic     string
	Partition int32
	Committed int64
	End       int64
	Lag       int64
}

// Describe is Describe's output (FUNC-SPEC §8.7 G2).
type Describe struct {
	ID            string
	State         string
	ProtocolType  string
	CoordinatorID int32
	Members       []MemberDetail
	Offsets       []OffsetDetail
	TotalLag      int64
}

// Describe returns groupID's full detail: members, per-partition committed/
// end/lag, and totalLag (FUNC-SPEC §8.7 G2). Unknown groupID →
// *kafka.Error{Kind: NotFound, Resource: "group"} (from Admin.DescribeGroups).
func (s *Service) Describe(ctx context.Context, groupID string) (Describe, error) {
	groups, err := s.admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return Describe{}, err
	}
	g := groups[0] // DescribeGroups(ctx, oneID) is exactly one result or an error.

	committed, err := s.admin.FetchGroupOffsets(ctx, groupID)
	if err != nil {
		return Describe{}, err
	}
	offsets, totalLag, err := s.resolveOffsetDetails(ctx, committed)
	if err != nil {
		return Describe{}, err
	}

	members := make([]MemberDetail, len(g.Members))
	for i, m := range g.Members {
		members[i] = MemberDetail{
			MemberID: m.MemberID, ClientID: m.ClientID, Host: m.Host,
			Assignments: append([]kafka.TopicPartition(nil), m.Assignments...),
		}
	}

	return Describe{
		ID: g.ID, State: g.State, ProtocolType: g.ProtocolType, CoordinatorID: g.CoordinatorID,
		Members: members, Offsets: offsets, TotalLag: totalLag,
	}, nil
}

// resolveOffsetDetails pairs committed's offsets with each referenced
// topic's current end offset (one Admin.ListEndOffsets call per distinct
// topic), sorted by topic then partition for a stable response.
func (s *Service) resolveOffsetDetails(ctx context.Context, committed map[kafka.TopicPartition]int64) ([]OffsetDetail, int64, error) {
	endByTopic := map[string]map[int32]int64{}
	for tp := range committed {
		if _, ok := endByTopic[tp.Topic]; ok {
			continue
		}
		end, err := s.admin.ListEndOffsets(ctx, tp.Topic)
		if err != nil {
			return nil, 0, err
		}
		endByTopic[tp.Topic] = end
	}

	tps := make([]kafka.TopicPartition, 0, len(committed))
	for tp := range committed {
		tps = append(tps, tp)
	}
	sort.Slice(tps, func(i, j int) bool {
		if tps[i].Topic != tps[j].Topic {
			return tps[i].Topic < tps[j].Topic
		}
		return tps[i].Partition < tps[j].Partition
	})

	var totalLag int64
	out := make([]OffsetDetail, len(tps))
	for i, tp := range tps {
		end := endByTopic[tp.Topic][tp.Partition]
		lag := max(end-committed[tp], 0)
		out[i] = OffsetDetail{Topic: tp.Topic, Partition: tp.Partition, Committed: committed[tp], End: end, Lag: lag}
		totalLag += lag
	}
	return out, totalLag, nil
}

// TopicGroupPartition is one partition of one group's consumption of a
// topic (FUNC-SPEC §8.7 G3).
type TopicGroupPartition struct {
	Partition int32
	Committed int64
	End       int64
	Lag       int64
}

// TopicGroup is one group consuming a topic (FUNC-SPEC §8.7 G3).
type TopicGroup struct {
	GroupID    string
	State      string
	TotalLag   int64
	Partitions []TopicGroupPartition
}

// ConsumersOfTopic finds every group with at least one committed offset on
// topic — the reverse lookup FUNC-SPEC §8.7 G3 names, independent of
// whether the group currently has any live, assigned members (a group
// stopped after committing still "consumes" the topic in this sense; the
// manual test plan stops its console consumer before checking G3).
// Unknown topic → *kafka.Error{Kind: NotFound, Resource: "topic"} (from
// Admin.ListEndOffsets).
func (s *Service) ConsumersOfTopic(ctx context.Context, topic string) ([]TopicGroup, error) {
	end, err := s.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}

	groups, err := s.admin.DescribeGroups(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]TopicGroup, 0, len(groups))
	for _, g := range groups {
		committed, err := s.admin.FetchGroupOffsets(ctx, g.ID)
		if err != nil {
			return nil, err
		}

		var totalLag int64
		var partitions []TopicGroupPartition
		for p, endOffset := range end {
			committedOffset, ok := committed[kafka.TopicPartition{Topic: topic, Partition: p}]
			if !ok {
				continue
			}
			lag := max(endOffset-committedOffset, 0)
			partitions = append(partitions, TopicGroupPartition{Partition: p, Committed: committedOffset, End: endOffset, Lag: lag})
			totalLag += lag
		}
		if len(partitions) == 0 {
			continue
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i].Partition < partitions[j].Partition })
		out = append(out, TopicGroup{GroupID: g.ID, State: g.State, TotalLag: totalLag, Partitions: partitions})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out, nil
}
