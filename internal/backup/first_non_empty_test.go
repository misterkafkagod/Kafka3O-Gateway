package backup

import "testing"

func TestFirstNonEmpty(t *testing.T) {
	if firstNonEmpty("a", "b") != "a" || firstNonEmpty("", "b") != "b" {
		t.Fatal("firstNonEmpty")
	}
}
