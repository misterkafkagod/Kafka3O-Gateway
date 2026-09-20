package franz

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/misterkafkagod/kafka3o/internal/kafka"
)

func TestFranz_KerrMapping_TableDriven(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		kerr *kerr.Error
		want kafka.Kind
	}{
		{"unknown topic or partition", kerr.UnknownTopicOrPartition, kafka.KindNotFound},
		{"group id not found", kerr.GroupIDNotFound, kafka.KindNotFound},
		{"topic already exists", kerr.TopicAlreadyExists, kafka.KindAlreadyExists},
		{"non-empty group", kerr.NonEmptyGroup, kafka.KindGroupActive},
		{"unsupported version", kerr.UnsupportedVersion, kafka.KindUnsupported},
		{"request timed out (unmapped)", kerr.RequestTimedOut, kafka.KindBroker},
		{"broker not available (unmapped)", kerr.BrokerNotAvailable, kafka.KindBroker},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := wrapErr("topic", tc.kerr)
			var pe *kafka.Error
			if !errors.As(err, &pe) {
				t.Fatalf("wrapErr(%v) = %v, want *kafka.Error", tc.kerr, err)
			}
			if pe.Kind != tc.want {
				t.Errorf("Kind = %v, want %v", pe.Kind, tc.want)
			}
			if pe.KafkaCode != tc.kerr.Code || pe.KafkaName != tc.kerr.Message {
				t.Errorf("code/name = %d/%q, want %d/%q", pe.KafkaCode, pe.KafkaName, tc.kerr.Code, tc.kerr.Message)
			}
			if !errors.Is(err, tc.kerr) {
				t.Error("Unwrap does not reach the original *kerr.Error")
			}
		})
	}
}

func TestFranz_KerrMapping_ContextDeadlineIsTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	err := wrapErr("topic", ctx.Err())
	var pe *kafka.Error
	if !errors.As(err, &pe) || pe.Kind != kafka.KindTimeout {
		t.Fatalf("wrapErr(DeadlineExceeded) = %v, want KindTimeout", err)
	}

	cancelled := wrapErr("", context.Canceled)
	if !kafka.IsKind(cancelled, kafka.KindTimeout) {
		t.Errorf("wrapErr(Canceled) Kind = %v, want KindTimeout", cancelled)
	}
}

func TestFranz_KerrMapping_NetworkFailureIsUnavailable(t *testing.T) {
	t.Parallel()
	netErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	err := wrapErr("", netErr)
	if !kafka.IsKind(err, kafka.KindUnavailable) {
		t.Errorf("wrapErr(net.OpError) Kind = %v, want KindUnavailable", err)
	}
}

func TestFranz_KerrMapping_UnknownErrorIsBroker(t *testing.T) {
	t.Parallel()
	err := wrapErr("topic", errors.New("boom"))
	var pe *kafka.Error
	if !errors.As(err, &pe) || pe.Kind != kafka.KindBroker || pe.KafkaCode != 0 || pe.KafkaName != "" {
		t.Errorf("wrapErr(plain error) = %+v, want KindBroker with no code/name", pe)
	}
}

func TestFranz_KerrMapping_NilStaysNil(t *testing.T) {
	t.Parallel()
	if wrapErr("topic", nil) != nil {
		t.Error("wrapErr(nil) is not nil")
	}
}
