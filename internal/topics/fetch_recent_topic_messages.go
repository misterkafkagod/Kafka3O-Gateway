package topics

import "context"

const TopicMessageSearchLimit = 500

type TopicMessageWindow struct {
	Messages  []TopicMessage
	Truncated bool
}

// FetchRecentTopicMessages returns records from at most TopicMessageSearchLimit offset positions.
type FetchRecentTopicMessages interface {
	Execute(ctx context.Context, topic string) (TopicMessageWindow, error)
}
