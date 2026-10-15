package api

import (
	"context"
	"net/http"
	"slices"
	"strings"
)

type topicSummary struct {
	Name           string `json:"name"`
	Partitions     int    `json:"partitions"`
	ApproxMessages int64  `json:"approxMessages"`
	Internal       bool   `json:"internal"`
}

type topicDetail struct {
	Name       string            `json:"name"`
	Partitions []partitionDetail `json:"partitions"`
	Configs    []configDetail    `json:"configs"`
}

type partitionDetail struct {
	ID              int32   `json:"id"`
	Leader          int32   `json:"leader"`
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	StartOffset     int64   `json:"startOffset"`
	EndOffset       int64   `json:"endOffset"`
	UnderReplicated bool    `json:"underReplicated"`
}

type configDetail struct {
	Key       string  `json:"key"`
	Value     *string `json:"value"`
	IsDefault bool    `json:"isDefault"`
	Sensitive bool    `json:"sensitive"`
}

// isInternal reports whether a topic is Kafka-internal; SPEC.md defines this
// as a name starting with "_" (e.g. __consumer_offsets, _schemas).
func isInternal(name string) bool { return strings.HasPrefix(name, "_") }

// approxMessages is the sum of (end - start) over partitions. It is approximate
// because compaction and transaction markers leave gaps in offsets.
func approxMessages(parts []PartitionInfo) int64 {
	var n int64
	for _, p := range parts {
		if p.EndOffset > p.StartOffset {
			n += p.EndOffset - p.StartOffset
		}
	}
	return n
}

func (s *Server) listTopics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.callTimeout)
	defer cancel()

	topics, err := s.cluster.ListTopics(ctx)
	if err != nil {
		s.log.Warn("list topics failed", "error", err)
		writeError(w, err)
		return
	}
	out := make([]topicSummary, 0, len(topics))
	for _, t := range topics {
		out = append(out, topicSummary{
			Name:           t.Name,
			Partitions:     len(t.Partitions),
			ApproxMessages: approxMessages(t.Partitions),
			Internal:       isInternal(t.Name),
		})
	}
	slices.SortFunc(out, func(a, b topicSummary) int { return strings.Compare(a.Name, b.Name) })
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) describeTopic(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.callTimeout)
	defer cancel()

	topic, configs, err := s.cluster.DescribeTopic(ctx, r.PathValue("name"))
	if err != nil {
		s.log.Warn("describe topic failed", "topic", r.PathValue("name"), "error", err)
		writeError(w, err)
		return
	}

	out := topicDetail{
		Name:       topic.Name,
		Partitions: make([]partitionDetail, 0, len(topic.Partitions)),
		Configs:    make([]configDetail, 0, len(configs)),
	}
	for _, p := range topic.Partitions {
		out.Partitions = append(out.Partitions, partitionDetail{
			ID:              p.ID,
			Leader:          p.Leader,
			Replicas:        nonNil(p.Replicas),
			ISR:             nonNil(p.ISR),
			StartOffset:     p.StartOffset,
			EndOffset:       p.EndOffset,
			UnderReplicated: len(p.ISR) < len(p.Replicas),
		})
	}
	slices.SortFunc(out.Partitions, func(a, b partitionDetail) int { return int(a.ID - b.ID) })

	for _, c := range configs {
		d := configDetail{Key: c.Key, Value: c.Value, IsDefault: c.IsDefault, Sensitive: c.Sensitive}
		if c.Sensitive {
			d.Value = nil // never send secrets to the browser
		}
		out.Configs = append(out.Configs, d)
	}
	slices.SortFunc(out.Configs, func(a, b configDetail) int { return strings.Compare(a.Key, b.Key) })

	writeJSON(w, http.StatusOK, out)
}

func nonNil(v []int32) []int32 {
	if v == nil {
		return []int32{}
	}
	return v
}
