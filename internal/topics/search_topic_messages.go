package topics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"
)

var ErrInvalidTopicMessageSearch = errors.New("search text must be non-empty UTF-8")

type SearchTopicMessages interface {
	Execute(ctx context.Context, topic, query string) (TopicMessageSearchResult, error)
}

type searchTopicMessages struct {
	fetch   FetchRecentTopicMessages
	timeout time.Duration
}

func NewSearchTopicMessages(fetch FetchRecentTopicMessages, timeout time.Duration) SearchTopicMessages {
	return searchTopicMessages{fetch: fetch, timeout: timeout}
}

func (s searchTopicMessages) Execute(ctx context.Context, topic, query string) (TopicMessageSearchResult, error) {
	if query == "" || !utf8.ValidString(query) {
		return TopicMessageSearchResult{}, ErrInvalidTopicMessageSearch
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	window, err := s.fetch.Execute(ctx, topic)
	if err != nil {
		return TopicMessageSearchResult{}, err
	}
	if len(window.Messages) > TopicMessageSearchLimit {
		return TopicMessageSearchResult{}, fmt.Errorf("search window contains %d records, limit is %d", len(window.Messages), TopicMessageSearchLimit)
	}
	matches := make([]TopicMessage, 0)
	needle := []byte(query)
	for _, message := range window.Messages {
		if bytes.Contains(message.Key, needle) || bytes.Contains(message.Value, needle) {
			matches = append(matches, message)
		}
	}
	slices.SortFunc(matches, CompareTopicMessagesByRecency)
	return TopicMessageSearchResult{Messages: matches, Scanned: len(window.Messages), Truncated: window.Truncated}, nil
}
