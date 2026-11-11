package backup

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateRestoreFilterWithoutFilter(t *testing.T) {
	window, partitions, err := ValidateRestoreFilter(RestoreFilter{})
	if err != nil || window != AllRecords || partitions != nil {
		t.Fatalf("%+v %v %v", window, partitions, err)
	}
}

func TestValidateRestoreFilterWindowAndPartitions(t *testing.T) {
	from := time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	window, partitions, err := ValidateRestoreFilter(RestoreFilter{From: &from, To: &to, Partitions: []int{2, 0, 2}})
	if err != nil || window != (RecordWindow{From: from.UnixMilli(), To: to.UnixMilli()}) || len(partitions) != 2 || partitions[0] != 0 || partitions[1] != 2 {
		t.Fatalf("%+v %v %v", window, partitions, err)
	}
	if window, _, _ := ValidateRestoreFilter(RestoreFilter{From: &from}); window.From != from.UnixMilli() || window.To != AllRecords.To {
		t.Fatalf("only a start: %+v", window)
	}
	if window, _, _ := ValidateRestoreFilter(RestoreFilter{To: &to}); window.From != AllRecords.From || window.To != to.UnixMilli() {
		t.Fatalf("only an end: %+v", window)
	}
}

func TestValidateRestoreFilterRefusals(t *testing.T) {
	from := time.Date(2026, 11, 11, 1, 0, 0, 0, time.UTC)
	before := from.Add(-time.Second)
	for name, filter := range map[string]RestoreFilter{
		"time range": {From: &from, To: &before},
		"empty":      {From: &from, To: &from},
		"partition":  {Partitions: []int{}},
		"negative":   {Partitions: []int{1, -1}},
	} {
		_, _, err := ValidateRestoreFilter(filter)
		if !errors.Is(err, ErrInvalidRestoreFilter) {
			t.Errorf("%s: %v", name, err)
		}
		if name == "time range" && !strings.Contains(err.Error(), "before its end") {
			t.Errorf("message: %v", err)
		}
	}
}
