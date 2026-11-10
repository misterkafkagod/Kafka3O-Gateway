package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type fakeCommitter struct {
	committed kadm.Offsets
	responses func(kadm.Offsets) kadm.OffsetResponses
	err       error
}

func (f *fakeCommitter) CommitOffsets(_ context.Context, _ string, offsets kadm.Offsets) (kadm.OffsetResponses, error) {
	f.committed = offsets
	if f.err != nil {
		return nil, f.err
	}
	if f.responses != nil {
		return f.responses(offsets), nil
	}
	responses := kadm.OffsetResponses{}
	offsets.Each(func(o kadm.Offset) { responses.Add(kadm.OffsetResponse{Offset: o}) })
	return responses, nil
}

func expected(offset int64) *int64 { return &offset }

// The fake log: orders partition 0 spans 2..10 with a commit at 7; partition 1 spans 3..20 without a commit.
var validReset = []groups.OffsetResetRequest{
	{Partition: 1, ExpectedOffset: nil, TargetOffset: 3},
	{Partition: 0, ExpectedOffset: expected(7), TargetOffset: 2},
}

func TestApplyOffsetResetCommitsThePreviewedTargets(t *testing.T) {
	admin := inactiveClassicAdmin()
	committer := &fakeCommitter{}
	result, err := NewCommitOffsetReset(NewEnsureGroupInactive(admin), admin, committer).Execute(context.Background(), "billing", "orders", validReset)
	if err != nil {
		t.Fatal(err)
	}
	if at, ok := committer.committed.Lookup("orders", 0); !ok || at.At != 2 {
		t.Fatalf("partition 0 committed %+v", at)
	}
	if at, ok := committer.committed.Lookup("orders", 1); !ok || at.At != 3 {
		t.Fatalf("partition 1 committed %+v", at)
	}
	if len(result.Partitions) != 2 || result.Partitions[0].Partition != 0 || result.Partitions[0].TargetOffset != 2 || result.Partitions[1].TargetOffset != 3 {
		t.Fatalf("result = %+v", result)
	}
}

func TestApplyOffsetResetRefusesStaleOrInvalidRequests(t *testing.T) {
	for name, tt := range map[string]struct {
		partitions []groups.OffsetResetRequest
		want       error
	}{
		"moved commit":       {[]groups.OffsetResetRequest{{Partition: 0, ExpectedOffset: expected(6), TargetOffset: 2}}, groups.ErrStalePreview},
		"new commit":         {[]groups.OffsetResetRequest{{Partition: 0, ExpectedOffset: nil, TargetOffset: 2}}, groups.ErrStalePreview},
		"vanished commit":    {[]groups.OffsetResetRequest{{Partition: 1, ExpectedOffset: expected(4), TargetOffset: 3}}, groups.ErrStalePreview},
		"before log start":   {[]groups.OffsetResetRequest{{Partition: 0, ExpectedOffset: expected(7), TargetOffset: 1}}, groups.ErrStalePreview},
		"after log end":      {[]groups.OffsetResetRequest{{Partition: 0, ExpectedOffset: expected(7), TargetOffset: 11}}, groups.ErrStalePreview},
		"unknown partition":  {[]groups.OffsetResetRequest{{Partition: 9, TargetOffset: 0}}, groups.ErrInvalidResetTarget},
		"repeated partition": {append(append([]groups.OffsetResetRequest{}, validReset...), validReset[0]), groups.ErrInvalidResetTarget},
		"no partitions":      {nil, groups.ErrInvalidResetTarget},
	} {
		admin := inactiveClassicAdmin()
		committer := &fakeCommitter{}
		if _, err := NewCommitOffsetReset(NewEnsureGroupInactive(admin), admin, committer).Execute(context.Background(), "billing", "orders", tt.partitions); !errors.Is(err, tt.want) {
			t.Errorf("%s: %v, want %v", name, err, tt.want)
		}
		if committer.committed != nil {
			t.Errorf("%s: committed %v", name, committer.committed)
		}
	}
}

func TestApplyOffsetResetRechecksMembershipAndTopic(t *testing.T) {
	active := inactiveClassicAdmin()
	active.described = kadm.DescribedGroups{"billing": {Group: "billing", Members: []kadm.DescribedGroupMember{{MemberID: "m"}}}}
	committer := &fakeCommitter{}
	if _, err := NewCommitOffsetReset(NewEnsureGroupInactive(active), active, committer).Execute(context.Background(), "billing", "orders", validReset); !errors.Is(err, groups.ErrGroupActive) || committer.committed != nil {
		t.Fatalf("active group: %v, committed %v", err, committer.committed)
	}
	missing := inactiveClassicAdmin()
	missing.ends = kadm.ListedOffsets{"orders": {-1: {Topic: "orders", Partition: -1, Err: kerr.UnknownTopicOrPartition}}}
	if _, err := NewCommitOffsetReset(NewEnsureGroupInactive(missing), missing, &fakeCommitter{}).Execute(context.Background(), "billing", "orders", validReset); !errors.Is(err, groups.ErrTopicNotFound) {
		t.Fatalf("unknown topic: %v", err)
	}
}

func TestApplyOffsetResetReportsCommitFailures(t *testing.T) {
	admin := inactiveClassicAdmin()
	lost := &fakeCommitter{err: errors.New("connection reset")}
	if _, err := NewCommitOffsetReset(NewEnsureGroupInactive(admin), admin, lost).Execute(context.Background(), "billing", "orders", validReset); !errors.Is(err, groups.ErrOutcomeUnknown) {
		t.Fatalf("lost response: %v", err)
	}
	failAll := func(code error) func(kadm.Offsets) kadm.OffsetResponses {
		return func(offsets kadm.Offsets) kadm.OffsetResponses {
			responses := kadm.OffsetResponses{}
			offsets.Each(func(o kadm.Offset) { responses.Add(kadm.OffsetResponse{Offset: o, Err: code}) })
			return responses
		}
	}
	joined := &fakeCommitter{responses: failAll(kerr.UnknownMemberID)}
	if _, err := NewCommitOffsetReset(NewEnsureGroupInactive(admin), admin, joined).Execute(context.Background(), "billing", "orders", validReset); !errors.Is(err, groups.ErrGroupActive) {
		t.Fatalf("group joined during the commit: %v", err)
	}
	rejected := &fakeCommitter{responses: failAll(kerr.GroupAuthorizationFailed)}
	if _, err := NewCommitOffsetReset(NewEnsureGroupInactive(admin), admin, rejected).Execute(context.Background(), "billing", "orders", validReset); err == nil ||
		errors.Is(err, groups.ErrOutcomeUnknown) || !errors.Is(err, kerr.GroupAuthorizationFailed) {
		t.Fatalf("rejected commit: %v", err)
	}
	partial := &fakeCommitter{responses: func(offsets kadm.Offsets) kadm.OffsetResponses {
		responses := kadm.OffsetResponses{}
		offsets.Each(func(o kadm.Offset) {
			response := kadm.OffsetResponse{Offset: o}
			if o.Partition == 1 {
				response.Err = kerr.RequestTimedOut
			}
			responses.Add(response)
		})
		return responses
	}}
	if _, err := NewCommitOffsetReset(NewEnsureGroupInactive(admin), admin, partial).Execute(context.Background(), "billing", "orders", validReset); !errors.Is(err, groups.ErrOutcomeUnknown) {
		t.Fatalf("partial commit: %v", err)
	}
}
