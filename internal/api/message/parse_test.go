package message_test

import (
	"testing"

	apimessage "github.com/misterkafkagod/kafka3o/internal/api/message"
	"github.com/misterkafkagod/kafka3o/internal/service/message"
)

func TestParseFrom_AllForms(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		in      string
		want    message.From
		wantErr bool
	}{
		{"beginning", "beginning", message.From{Kind: message.FromBeginning}, false},
		{"latest", "latest", message.From{Kind: message.FromLatest}, false},
		{"offset:n", "offset:42", message.From{Kind: message.FromOffset, Offset: 42}, false},
		{"timestamp:ms", "timestamp:1700000000000", message.From{Kind: message.FromTimestamp, TimeMilli: 1700000000000}, false},
		{"timestamp:iso", "timestamp:2023-11-14T22:13:20Z", message.From{Kind: message.FromTimestamp, TimeMilli: 1700000000000}, false},
		{"invalid keyword", "whenever", message.From{}, true},
		{"invalid offset", "offset:not-a-number", message.From{}, true},
		{"invalid timestamp", "timestamp:not-a-time", message.From{}, true},
		{"empty", "", message.From{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := apimessage.ParseFrom(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseFrom(%q) = %+v, nil, want an error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFrom(%q) error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseFrom(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}
