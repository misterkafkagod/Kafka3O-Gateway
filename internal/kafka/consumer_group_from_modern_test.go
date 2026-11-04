package kafka

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestConsumerGroupFromModern(t *testing.T) {
	listed := kadm.ListedGroup{Group: "modern", State: "Empty"}
	described := kadm.DescribedConsumerGroup{
		State: "Stable",
		Members: []kadm.ConsumerGroupMember{{
			MemberID: "member", SubscribedTopics: []string{"subscribed"},
			Assignment: kadm.TopicsSet{"assigned": {3: {}}},
		}},
	}
	got := consumerGroupFromModern(listed, &described, nil)
	if got.snapshot.Protocol != "consumer" || got.snapshot.State != "Stable" || len(got.snapshot.Members) != 1 ||
		len(got.topics["subscribed"]) != 0 || !got.topics.Lookup("assigned", 3) {
		t.Fatalf("group = %#v", got)
	}
	if _, ok := got.topics["subscribed"]; !ok {
		t.Fatal("explicit subscription is missing")
	}
	described.Err = errors.New("not authorized")
	failed := consumerGroupFromModern(listed, &described, nil)
	if len(failed.problems) != 1 || !strings.Contains(failed.problems[0], "not authorized") {
		t.Errorf("problems = %v", failed.problems)
	}
}
