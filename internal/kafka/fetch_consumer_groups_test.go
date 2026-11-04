package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

type fakeConsumerGroupsAdmin struct {
	classic       kadm.ListedGroups
	modern        kadm.ListedGroups
	classicDesc   kadm.DescribedGroups
	modernDesc    kadm.DescribedConsumerGroups
	classicErr    error
	modernErr     error
	listErr       error
	fetched       kadm.FetchOffsetsResponses
	starts        kadm.ListedOffsets
	ends          kadm.ListedOffsets
	startErr      error
	endErr        error
	startRequests [][]string
	endRequests   [][]string
}

func (f *fakeConsumerGroupsAdmin) ListGroupsByType(_ context.Context, types []string, _ ...string) (kadm.ListedGroups, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if len(types) == 1 && types[0] == "classic" {
		return f.classic, nil
	}
	if len(types) == 1 && types[0] == "consumer" {
		return f.modern, nil
	}
	return nil, errors.New("unexpected group type")
}

func (f *fakeConsumerGroupsAdmin) DescribeGroups(_ context.Context, _ ...string) (kadm.DescribedGroups, error) {
	return f.classicDesc, f.classicErr
}

func (f *fakeConsumerGroupsAdmin) DescribeConsumerGroups(_ context.Context, _ ...string) (kadm.DescribedConsumerGroups, error) {
	return f.modernDesc, f.modernErr
}

func (f *fakeConsumerGroupsAdmin) FetchManyOffsets(_ context.Context, _ ...string) kadm.FetchOffsetsResponses {
	return f.fetched
}

func (f *fakeConsumerGroupsAdmin) ListStartOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	f.startRequests = append(f.startRequests, topics)
	return f.starts, f.startErr
}

func (f *fakeConsumerGroupsAdmin) ListEndOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	f.endRequests = append(f.endRequests, topics)
	return f.ends, f.endErr
}

func TestFetchConsumerGroups_CombinesProtocolsSubscriptionsAndOffsets(t *testing.T) {
	admin := &fakeConsumerGroupsAdmin{
		classic: kadm.ListedGroups{
			"idle":    {Group: "idle", State: "Empty"},
			"connect": {Group: "connect", ProtocolType: "connect"},
		},
		modern:      kadm.ListedGroups{"modern": {Group: "modern"}},
		classicDesc: kadm.DescribedGroups{"idle": {Group: "idle", State: "Empty"}},
		modernDesc: kadm.DescribedConsumerGroups{"modern": {Group: "modern", State: "Stable", Members: []kadm.ConsumerGroupMember{{
			MemberID: "m", SubscribedTopics: []string{"orders"}, Assignment: kadm.TopicsSet{"alerts": {2: {}}},
		}}}},
		fetched: kadm.FetchOffsetsResponses{
			"idle": {Fetched: kadm.OffsetResponses{"orders": {0: {Offset: kadm.Offset{At: 5}}}}},
			"modern": {Fetched: kadm.OffsetResponses{
				"orders": {0: {Offset: kadm.Offset{At: 8}}},
			}},
		},
		starts: kadm.ListedOffsets{"orders": {0: {Offset: 0}, 1: {Offset: 0}}, "alerts": {2: {Offset: 3}}},
		ends:   kadm.ListedOffsets{"orders": {0: {Offset: 10}, 1: {Offset: 4}}, "alerts": {2: {Offset: 7}}},
	}
	got, err := groups.NewListConsumerGroups(NewFetchConsumerGroups(admin), time.Second).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "idle" || got[1].ID != "modern" {
		t.Fatalf("groups = %#v", got)
	}
	if got[0].Protocol != "classic" || len(got[0].Partitions) != 2 ||
		got[0].Partitions[0].Status != "ok" || *got[0].Partitions[0].Lag != "5" ||
		got[0].Partitions[1].Status != "no_commit" {
		t.Errorf("inactive group = %#v", got[0])
	}
	if got[1].Protocol != "consumer" || len(got[1].Members) != 1 || len(got[1].Topics) != 2 ||
		got[1].Topics[0] != "alerts" || len(got[1].Partitions) != 3 ||
		got[1].Partitions[0].Status != "no_commit" || got[1].Partitions[1].Status != "ok" ||
		*got[1].Partitions[1].Lag != "2" || got[1].Partitions[2].Status != "no_commit" {
		t.Errorf("modern group = %#v", got[1])
	}
	if len(admin.startRequests) != 1 || len(admin.startRequests[0]) != 2 || len(admin.endRequests) != 1 {
		t.Errorf("offset requests = %v / %v", admin.startRequests, admin.endRequests)
	}
}

func TestFetchConsumerGroups_ReportsGroupFailuresWithoutInventingLag(t *testing.T) {
	admin := &fakeConsumerGroupsAdmin{
		classic:    kadm.ListedGroups{"failed": {Group: "failed"}},
		classicErr: errors.New("describe denied"),
		modern:     kadm.ListedGroups{"partial": {Group: "partial"}},
		modernDesc: kadm.DescribedConsumerGroups{"partial": {Group: "partial", Members: []kadm.ConsumerGroupMember{{SubscribedTopics: []string{"orders"}}}}},
		fetched:    kadm.FetchOffsetsResponses{"failed": {Err: errors.New("commit denied")}},
		starts:     kadm.ListedOffsets{"orders": {0: {Offset: 0}}},
		ends:       kadm.ListedOffsets{"orders": {0: {Offset: 10}}},
	}
	got, err := groups.NewListConsumerGroups(NewFetchConsumerGroups(admin), time.Second).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !strings.Contains(got[0].Error, "describe denied") || !strings.Contains(got[0].Error, "commit denied") {
		t.Fatalf("failed group = %#v", got)
	}
	if !strings.Contains(got[1].Error, "missing group offset response") || len(got[1].Partitions) != 1 ||
		got[1].Partitions[0].Status != "unavailable" || got[1].Partitions[0].Lag != nil {
		t.Errorf("partial group = %#v", got[1])
	}
}

func TestFetchConsumerGroups_EmptyAndRequestErrors(t *testing.T) {
	admin := &fakeConsumerGroupsAdmin{}
	got, err := NewFetchConsumerGroups(admin).Execute(context.Background())
	if err != nil || got == nil || len(got) != 0 || len(admin.startRequests) != 0 || len(admin.endRequests) != 0 {
		t.Errorf("empty groups = %#v, error = %v; unexpected offset requests", got, err)
	}
	failure := errors.New("offline")
	admin.listErr = failure
	if _, err := NewFetchConsumerGroups(admin).Execute(context.Background()); !errors.Is(err, failure) {
		t.Errorf("list error = %v", err)
	}
	admin.listErr = nil
	admin.classic = kadm.ListedGroups{"duplicate": {Group: "duplicate"}}
	admin.modern = kadm.ListedGroups{"duplicate": {Group: "duplicate"}}
	if _, err := NewFetchConsumerGroups(admin).Execute(context.Background()); err == nil {
		t.Error("expected an error for a group reported under both protocols")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	admin.modern = nil
	if _, err := NewFetchConsumerGroups(admin).Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled error = %v", err)
	}
}
