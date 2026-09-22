package core

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// Token is the plan token (FUNC-SPEC V5, TECH-SPEC C8): a deterministic,
// order-independent, duplicate-collapsing digest of commandID plus targets,
// returned by a multi-target destructive command's dry-run (T8, C9, C12) and
// re-derived on execute to detect a target set that has since changed.
//
// The canonical form is commandID + "\n" + the sorted, de-duplicated targets
// joined by "\n" — lower-case hex SHA-256 of that string, 64 characters.
func Token(commandID string, targets []string) string {
	sorted := make([]string, len(targets))
	copy(sorted, targets)
	sort.Strings(sorted)

	unique := sorted[:0]
	var prev string
	for i, t := range sorted {
		if i == 0 || t != prev {
			unique = append(unique, t)
		}
		prev = t
	}

	canonical := commandID + "\n" + strings.Join(unique, "\n")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
