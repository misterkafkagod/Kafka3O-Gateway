package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/groups"
)

func TestEnsureGroupInactive(t *testing.T) {
	inactive := inactiveClassicAdmin()
	if err := NewEnsureGroupInactive(inactive).Execute(context.Background(), "billing"); err != nil {
		t.Fatalf("inactive classic group: %v", err)
	}
	active := inactiveClassicAdmin()
	active.described = kadm.DescribedGroups{"billing": {Group: "billing", Members: []kadm.DescribedGroupMember{{MemberID: "m"}}}}
	if err := NewEnsureGroupInactive(active).Execute(context.Background(), "billing"); !errors.Is(err, groups.ErrGroupActive) {
		t.Fatalf("active classic group: %v", err)
	}
	if err := NewEnsureGroupInactive(inactive).Execute(context.Background(), "missing"); !errors.Is(err, groups.ErrGroupNotFound) {
		t.Fatalf("unknown group: %v", err)
	}
	undescribed := inactiveClassicAdmin()
	undescribed.described = kadm.DescribedGroups{}
	if err := NewEnsureGroupInactive(undescribed).Execute(context.Background(), "billing"); err == nil || errors.Is(err, groups.ErrGroupActive) {
		t.Fatalf("missing description: %v", err)
	}
}
