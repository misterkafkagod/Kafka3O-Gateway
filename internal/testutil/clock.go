package testutil

import (
	"time"

	"github.com/misterkafkagod/kafka3o/internal/kafka/fake"
)

// WithClock fixes the clock the underlying fake stamps seeded and produced
// records with (TECH-SPEC §4.9), so a test can assert on exact timestamps
// instead of a time window. It must run before the fake is constructed, so
// — unlike the other options — it is applied to fake.New itself rather than
// as a post-construction hook.
func WithClock(now func() time.Time) Option {
	return func(s *settings) { s.fakeOpts = append(s.fakeOpts, fake.WithClock(now)) }
}
