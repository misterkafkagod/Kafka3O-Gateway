package franz

import (
	"context"
	"errors"
	"net"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

// classify maps the named kerr.Error codes the catalog cares about onto a
// kafka.Kind and, where relevant, the resource for the API layer's
// details.resource (FUNC-SPEC §8.4; TECH-SPEC L2). Any other broker error
// falls through to KindBroker with no resource, still carrying code and name.
func classify(code int16) (kind kafka.Kind, resource string) {
	switch code {
	case kerr.UnknownTopicOrPartition.Code:
		return kafka.KindNotFound, "topic"
	case kerr.GroupIDNotFound.Code:
		return kafka.KindNotFound, "group"
	case kerr.TopicAlreadyExists.Code:
		return kafka.KindAlreadyExists, "topic"
	case kerr.NonEmptyGroup.Code:
		return kafka.KindGroupActive, "group"
	case kerr.UnsupportedVersion.Code:
		return kafka.KindUnsupported, ""
	case kerr.ResourceNotFound.Code:
		return kafka.KindNotFound, ""
	default:
		return kafka.KindBroker, ""
	}
}

// wrapErr maps a franz-go / kadm error onto *kafka.Error (TECH-SPEC L2, L3):
//
//   - nil stays nil.
//   - A *kerr.Error (or kadm's *AuthError wrapping one) maps via kindByCode,
//     defaulting to KindBroker; code and name are always preserved.
//   - A context deadline or cancellation maps to KindTimeout.
//   - A network-level failure (dial/read/write to no broker) maps to
//     KindUnavailable.
//   - Anything else maps to KindBroker with no code or name.
func wrapErr(resource string, err error) error {
	if err == nil {
		return nil
	}

	var ke *kerr.Error
	if errors.As(err, &ke) {
		kind, res := classify(ke.Code)
		if resource != "" {
			res = resource
		}
		return &kafka.Error{Kind: kind, Resource: res, KafkaCode: ke.Code, KafkaName: ke.Message, Cause: err}
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &kafka.Error{Kind: kafka.KindTimeout, Resource: resource, Cause: err}
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return &kafka.Error{Kind: kafka.KindUnavailable, Resource: resource, Cause: err}
	}

	return &kafka.Error{Kind: kafka.KindBroker, Resource: resource, Cause: err}
}
