package connect

import (
	"slices"
	"testing"
)

func TestCompareConnectors(t *testing.T) {
	connectors := []Connector{{Name: "b"}, {Name: "a"}, {Name: "c"}}
	slices.SortFunc(connectors, CompareConnectors)
	if connectors[0].Name != "a" || connectors[2].Name != "c" {
		t.Fatalf("connectors = %+v", connectors)
	}
}
