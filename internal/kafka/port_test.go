package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

// The compile-time assertions that the adapters satisfy the role interfaces live
// beside each adapter (fake_test.go, franz_test.go — Tasks 1.6 and 1.5), because
// this package may import neither (TECH-SPEC D1, §5.3). Here we assert that the
// interfaces are satisfiable by a trivial type, so their method sets are sane.
type nopAdmin struct{}

func (nopAdmin) DescribeCluster(context.Context) (ClusterInfo, error) { return ClusterInfo{}, nil }

var (
	_ Admin    = nopAdmin{}
	_ Consumer = struct{}{}
	_ Producer = struct{}{}
)

func TestError_ErrorsAsExposesKindCodeName(t *testing.T) {
	t.Parallel()
	cause := io.ErrUnexpectedEOF
	err := fmt.Errorf("adapter: %w", &Error{
		Kind: KindNotFound, Resource: "topic", KafkaCode: 3, KafkaName: "UNKNOWN_TOPIC_OR_PARTITION", Cause: cause,
	})

	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("errors.As failed on %v", err)
	}
	if pe.Kind != KindNotFound || pe.Resource != "topic" || pe.KafkaCode != 3 || pe.KafkaName != "UNKNOWN_TOPIC_OR_PARTITION" {
		t.Errorf("fields not preserved: %+v", pe)
	}
	if !errors.Is(err, cause) {
		t.Error("Unwrap does not expose the cause")
	}
	if k, ok := KindOf(err); !ok || k != KindNotFound {
		t.Errorf("KindOf = %v, %v", k, ok)
	}
	if !IsKind(err, KindNotFound) || IsKind(err, KindTimeout) {
		t.Error("IsKind mismatch")
	}
	if _, ok := KindOf(errors.New("plain")); ok {
		t.Error("KindOf matched a non-port error")
	}

	msg := pe.Error()
	for _, want := range []string{"not_found", "topic", "UNKNOWN_TOPIC_OR_PARTITION/3", cause.Error()} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, missing %q", msg, want)
		}
	}
	if got := (&Error{Kind: KindUnavailable}).Error(); got != "kafka: unavailable" {
		t.Errorf("minimal Error() = %q", got)
	}
}

func TestKind_StringAndKindsAreComplete(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for _, k := range Kinds() {
		s := k.String()
		if strings.HasPrefix(s, "kind(") {
			t.Errorf("Kind %d has no name", int(k))
		}
		if seen[s] {
			t.Errorf("duplicate Kind name %q", s)
		}
		seen[s] = true
	}
	if len(Kinds()) != 8 {
		t.Errorf("Kinds() has %d entries, want 8 (FUNC-SPEC §8.4 mapping)", len(Kinds()))
	}
	if got := Kind(99).String(); got != "kind(99)" {
		t.Errorf("unknown Kind String() = %q", got)
	}
}

func TestTypes_NoJSONTags(t *testing.T) {
	t.Parallel()
	types := []reflect.Type{
		reflect.TypeFor[Broker](), reflect.TypeFor[ClusterInfo](), reflect.TypeFor[Topic](),
		reflect.TypeFor[Partition](), reflect.TypeFor[TopicPartition](), reflect.TypeFor[Record](),
		reflect.TypeFor[Header](), reflect.TypeFor[Group](), reflect.TypeFor[GroupMember](),
		reflect.TypeFor[ConfigEntry](), reflect.TypeFor[Error](),
	}
	for _, typ := range types {
		for i := range typ.NumField() {
			f := typ.Field(i)
			for _, tag := range []string{"json", "yaml", "koanf", "validate", "query", "path", "header"} {
				if _, ok := f.Tag.Lookup(tag); ok {
					t.Errorf("%s.%s carries a %q tag; domain types must be tag-free (TECH-SPEC I5)", typ.Name(), f.Name, tag)
				}
			}
		}
	}
}
