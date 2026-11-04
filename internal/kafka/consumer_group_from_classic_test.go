package kafka

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestConsumerGroupFromClassic(t *testing.T) {
	listed := kadm.ListedGroup{Group: "classic", State: "Empty"}
	instance := "instance"
	described := kadm.DescribedGroup{
		State: "Stable",
		Members: []kadm.DescribedGroupMember{{
			MemberID: "member", ClientID: "client", ClientHost: "host", InstanceID: &instance,
		}},
	}
	got := consumerGroupFromClassic(listed, &described, nil)
	if got.snapshot.ID != "classic" || got.snapshot.Protocol != "classic" || got.snapshot.State != "Stable" ||
		len(got.snapshot.Members) != 1 || got.snapshot.Members[0].ID != "member" || *got.snapshot.Members[0].InstanceID != instance {
		t.Fatalf("group = %#v", got)
	}
	failed := consumerGroupFromClassic(listed, nil, errors.New("coordinator unavailable"))
	if len(failed.problems) != 1 || !strings.Contains(failed.problems[0], "coordinator unavailable") {
		t.Errorf("problems = %v", failed.problems)
	}
}
