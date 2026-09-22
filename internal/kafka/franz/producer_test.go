package franz

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzProducer_OptionsAcksAllIdempotent(t *testing.T) {
	t.Parallel()

	// Construction only, no broker: New already applies RequiredAcks(AllISRAcks())
	// and never calls DisableIdempotentWrite (kgo's idempotence default is
	// on) -- TECH-SPEC C4. Building a client with these options must not
	// require a live broker.
	c, err := New(Config{Bootstrap: []string{"127.0.0.1:0"}})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer c.Close()
}

func TestExplicitOrDefaultPartitioner(t *testing.T) {
	t.Parallel()

	t.Run("explicit partition returned verbatim, even out of range", func(t *testing.T) {
		t.Parallel()
		tp := explicitOrDefaultPartitioner{}.ForTopic("t")
		r := &kgo.Record{Partition: 5}
		if got := tp.Partition(r, 3); got != 5 {
			t.Errorf("Partition() = %d, want 5 (verbatim, out of range)", got)
		}
	})

	t.Run("keyless records spread round-robin within bounds", func(t *testing.T) {
		t.Parallel()
		tp := explicitOrDefaultPartitioner{}.ForTopic("t")
		seen := map[int]bool{}
		for range 9 {
			r := &kgo.Record{Partition: unsetPartition}
			p := tp.Partition(r, 3)
			if p < 0 || p >= 3 {
				t.Fatalf("Partition() = %d, want in [0,3)", p)
			}
			seen[p] = true
		}
		if len(seen) != 3 {
			t.Errorf("round-robin visited %d distinct partitions over 9 calls, want 3", len(seen))
		}
	})

	t.Run("same key hashes to the same partition", func(t *testing.T) {
		t.Parallel()
		tp := explicitOrDefaultPartitioner{}.ForTopic("t")
		r1 := &kgo.Record{Partition: unsetPartition, Key: []byte("stable-key")}
		r2 := &kgo.Record{Partition: unsetPartition, Key: []byte("stable-key")}
		p1 := tp.Partition(r1, 5)
		p2 := tp.Partition(r2, 5)
		if p1 != p2 {
			t.Errorf("same key hashed to different partitions: %d vs %d", p1, p2)
		}
	})
}
