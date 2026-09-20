package command

import (
	"slices"
	"testing"
)

func ids(filter func(Descriptor) bool) []string {
	var out []string
	for _, d := range Table() {
		if filter(d) {
			out = append(out, d.ID)
		}
	}
	slices.Sort(out)
	return out
}

func TestTable_Has41UniqueIDs(t *testing.T) {
	t.Parallel()
	tbl := Table()
	if len(tbl) != 41 {
		t.Fatalf("Table() has %d entries, want 41 (FUNC-SPEC §5)", len(tbl))
	}
	seen := map[string]bool{}
	for _, d := range tbl {
		if d.ID == "" || d.Name == "" {
			t.Errorf("descriptor %+v has an empty ID or Name", d)
		}
		if seen[d.ID] {
			t.Errorf("duplicate ID %q", d.ID)
		}
		seen[d.ID] = true
		if d.Access != R && d.Access != W {
			t.Errorf("%s: access %q is neither R nor W", d.ID, d.Access)
		}
	}
	// Every catalog family is present with the expected count: C1–C12, T1–T12, M1–M8, G1–G7, S1–S2.
	counts := map[byte]int{}
	for id := range seen {
		counts[id[0]]++
	}
	want := map[byte]int{'C': 12, 'T': 12, 'M': 8, 'G': 7, 'S': 2}
	for family, n := range want {
		if counts[family] != n {
			t.Errorf("family %c has %d commands, want %d", family, counts[family], n)
		}
	}
}

func TestTable_DestructiveSetMatchesSpec(t *testing.T) {
	t.Parallel()
	want := []string{"C12", "C5", "C9", "G4", "G5", "G6", "G7", "M8", "S1", "S2", "T10", "T11", "T12", "T7", "T8", "T9"}
	got := ids(func(d Descriptor) bool { return d.Destructive })
	if !slices.Equal(got, want) {
		t.Errorf("destructive set = %v, want FUNC-SPEC §5.6 %v", got, want)
	}
}

func TestTable_DataPlaneIsM1ToM8(t *testing.T) {
	t.Parallel()
	want := []string{"M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8"}
	got := ids(func(d Descriptor) bool { return d.DataPlane })
	if !slices.Equal(got, want) {
		t.Errorf("data-plane set = %v, want FUNC-SPEC F6 %v", got, want)
	}
}

func TestTable_DestructiveImpliesW(t *testing.T) {
	t.Parallel()
	for _, d := range Table() {
		if d.Destructive && d.Access != W {
			t.Errorf("%s is destructive but has access %q; §9.1 rules require W", d.ID, d.Access)
		}
	}
}

func TestTable_AccessMatchesCatalog(t *testing.T) {
	t.Parallel()
	// The R set from FUNC-SPEC §5.1–§5.3; everything else is W.
	wantR := []string{"C1", "C10", "C11", "C2", "C3", "C4", "C6", "C7", "C8",
		"G1", "G2", "G3", "M1", "M2", "M3", "M4", "T1", "T2", "T3", "T4"}
	got := ids(func(d Descriptor) bool { return d.Access == R })
	if !slices.Equal(got, wantR) {
		t.Errorf("R set = %v, want %v", got, wantR)
	}
	if n := len(ids(func(d Descriptor) bool { return d.Access == W })); n != 21 {
		t.Errorf("W set has %d commands, want 21", n)
	}
}

func TestTable_ReturnsFreshCopy(t *testing.T) {
	t.Parallel()
	a := Table()
	a[0].ID = "mutated"
	if b := Table(); b[0].ID != "C1" {
		t.Errorf("Table() shares state across calls: %q", b[0].ID)
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()
	d, ok := Lookup("T7")
	if !ok || d.Name != "Delete topic" || !d.Destructive || d.Access != W {
		t.Errorf("Lookup(T7) = %+v, %v", d, ok)
	}
	if _, ok := Lookup("T99"); ok {
		t.Error("Lookup(T99) found a descriptor")
	}
}
