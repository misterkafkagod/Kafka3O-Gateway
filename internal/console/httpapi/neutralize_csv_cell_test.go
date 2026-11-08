package httpapi

import "testing"

func TestNeutralizeCSVCell(t *testing.T) {
	for in, want := range map[string]string{
		"=HYPERLINK(1)": "'=HYPERLINK(1)", "+1": "'+1", "-2": "'-2", "@SUM": "'@SUM", "\tx": "'\tx", "\rx": "'\rx",
		"messages.produce": "messages.produce", "": "", "a=b": "a=b",
	} {
		if got := neutralizeCSVCell(in); got != want {
			t.Errorf("neutralizeCSVCell(%q) = %q, want %q", in, got, want)
		}
	}
}
