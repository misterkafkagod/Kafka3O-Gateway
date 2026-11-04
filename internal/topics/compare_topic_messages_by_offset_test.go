package topics

import "testing"

func TestCompareTopicMessagesByOffset(t *testing.T) {
	tests := []struct {
		name string
		a    TopicMessage
		b    TopicMessage
		want int
	}{
		{name: "earlier offset", a: TopicMessage{Offset: 3}, b: TopicMessage{Offset: 7}, want: -1},
		{name: "later offset", a: TopicMessage{Offset: 7}, b: TopicMessage{Offset: 3}, want: 1},
		{name: "same offset", a: TopicMessage{Offset: 3}, b: TopicMessage{Offset: 3}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareTopicMessagesByOffset(tt.a, tt.b); got != tt.want {
				t.Errorf("CompareTopicMessagesByOffset() = %d, want %d", got, tt.want)
			}
		})
	}
}
