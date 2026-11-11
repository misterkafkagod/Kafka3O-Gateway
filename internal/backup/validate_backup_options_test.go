package backup

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateBackupOptionsDefaults(t *testing.T) {
	for _, options := range []*BackupOptions{nil, {}} {
		got, err := ValidateBackupOptions(options)
		if err != nil || got != (BackupOptions{StartFrom: "earliest", Compression: "gzip"}) {
			t.Errorf("%+v: %+v %v", options, got, err)
		}
	}
}

func TestValidateBackupOptionsAcceptsEveryChoice(t *testing.T) {
	for _, start := range []string{"earliest", "latest"} {
		for _, compression := range []string{"gzip", "zstd", "snappy", "none"} {
			for _, records := range []int{0, 1, MaxRecordsPerFile} {
				want := BackupOptions{StartFrom: start, Compression: compression, RecordsPerFile: records}
				if got, err := ValidateBackupOptions(&want); err != nil || got != want {
					t.Errorf("%+v: %+v %v", want, got, err)
				}
			}
		}
	}
}

func TestValidateBackupOptionsNamesTheInvalidField(t *testing.T) {
	for field, options := range map[string]BackupOptions{
		"startFrom":      {StartFrom: "none"},
		"compression":    {Compression: "lz4"},
		"recordsPerFile": {RecordsPerFile: MaxRecordsPerFile + 1},
	} {
		_, err := ValidateBackupOptions(&options)
		if !errors.Is(err, ErrInvalidBackupSettings) || !strings.Contains(err.Error(), field) {
			t.Errorf("%s: %v", field, err)
		}
	}
	if _, err := ValidateBackupOptions(&BackupOptions{RecordsPerFile: -1}); !errors.Is(err, ErrInvalidBackupSettings) {
		t.Errorf("negative: %v", err)
	}
}
