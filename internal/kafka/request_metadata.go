package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func requestMetadata(ctx context.Context, requester kmsg.Requestor, includeTopics bool) (*kmsg.MetadataResponse, error) {
	request := kmsg.NewPtrMetadataRequest()
	request.AllowAutoTopicCreation = false
	if !includeTopics {
		request.Topics = []kmsg.MetadataRequestTopic{}
	}
	response, err := request.RequestWith(ctx, requester)
	if err != nil {
		return nil, fmt.Errorf("request metadata: %w", err)
	}
	if response == nil {
		return nil, errors.New("request metadata: empty response")
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		return nil, fmt.Errorf("request metadata: %w", err)
	}
	return response, nil
}
