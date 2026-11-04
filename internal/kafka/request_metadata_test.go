package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fakeMetadataRequestor struct {
	response *kmsg.MetadataResponse
	err      error
	requests []*kmsg.MetadataRequest
	ctx      context.Context
}

func (f *fakeMetadataRequestor) Request(ctx context.Context, request kmsg.Request) (kmsg.Response, error) {
	metadataRequest, ok := request.(*kmsg.MetadataRequest)
	if !ok {
		return nil, fmt.Errorf("unexpected request %T", request)
	}
	f.ctx = ctx
	f.requests = append(f.requests, metadataRequest)
	return f.response, f.err
}

func TestRequestMetadata_RequestsBrokersOrAllTopicsWithoutAutocreation(t *testing.T) {
	requester := &fakeMetadataRequestor{response: &kmsg.MetadataResponse{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := requestMetadata(ctx, requester, false); err != nil {
		t.Fatal(err)
	}
	if _, err := requestMetadata(ctx, requester, true); err != nil {
		t.Fatal(err)
	}
	if requester.ctx != ctx {
		t.Error("request context was not forwarded")
	}
	if requester.requests[0].Topics == nil || len(requester.requests[0].Topics) != 0 {
		t.Errorf("broker request topics = %#v, want non-nil empty slice", requester.requests[0].Topics)
	}
	if requester.requests[1].Topics != nil {
		t.Errorf("all-topics request topics = %#v, want nil", requester.requests[1].Topics)
	}
	for _, request := range requester.requests {
		if request.AllowAutoTopicCreation {
			t.Error("metadata request must not auto-create topics")
		}
	}
}

func TestRequestMetadata_PropagatesRequestAndBrokerErrors(t *testing.T) {
	offline := errors.New("broker unavailable")
	requester := &fakeMetadataRequestor{err: offline}
	if _, err := requestMetadata(context.Background(), requester, false); !errors.Is(err, offline) {
		t.Errorf("request error = %v, want %v", err, offline)
	}

	requester.err = nil
	requester.response = &kmsg.MetadataResponse{ErrorCode: kerr.ClusterAuthorizationFailed.Code}
	if _, err := requestMetadata(context.Background(), requester, false); !errors.Is(err, kerr.ClusterAuthorizationFailed) {
		t.Errorf("broker error = %v, want %v", err, kerr.ClusterAuthorizationFailed)
	}

	requester.response = nil
	if _, err := requestMetadata(context.Background(), requester, false); err == nil {
		t.Error("expected an error for an empty metadata response")
	}
}
