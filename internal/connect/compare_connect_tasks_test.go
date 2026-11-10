package connect

import (
	"slices"
	"testing"
)

func TestCompareConnectTasks(t *testing.T) {
	tasks := []Task{{ID: 2}, {ID: 0}, {ID: 1}}
	slices.SortFunc(tasks, CompareConnectTasks)
	if tasks[0].ID != 0 || tasks[2].ID != 2 {
		t.Fatalf("tasks = %+v", tasks)
	}
}
