package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	writeJournalSchema     = "statediff-rewriter-write-journal/v1"
	writeJournalIssued     = "issued"
	writeJournalObserved   = "observed"
	writeOutcomeConfirmed  = "confirmed"
	writeOutcomeUncertain  = "uncertain"
	writeOutcomeReconciled = "reconciled"
)

type writeIntent struct {
	Operation     string `json:"operation"`
	Ordinal       uint64 `json:"ordinal"`
	Height        uint64 `json:"height"`
	Key           string `json:"key"`
	OldSHA256     string `json:"expected_old_sha256"`
	NewSHA256     string `json:"expected_new_sha256"`
	IfMatch       string `json:"if_match"`
	PUTAttempts   uint64 `json:"put_attempts"`
	ConfirmedPUTs uint64 `json:"confirmed_puts"`
	UncertainPUTs uint64 `json:"uncertain_puts_verified"`
	Conflicts     uint64 `json:"conditional_conflicts,omitempty"`
}

type writeObservedResult struct {
	Ordinal        uint64 `json:"ordinal"`
	ObservedSHA256 string `json:"observed_sha256"`
	PostPUTETag    string `json:"post_put_etag"`
	Outcome        string `json:"outcome"`
}

type writeJournal struct {
	Schema        string                `json:"schema"`
	Checksum      string                `json:"checksum"`
	RunID         string                `json:"run_id"`
	ManifestHash  string                `json:"manifest_sha256"`
	Operation     string                `json:"operation"`
	Start         uint64                `json:"start_ordinal"`
	End           uint64                `json:"end_ordinal"`
	BatchRecords  uint64                `json:"batch_records"`
	BatchBytes    int64                 `json:"batch_estimated_bytes"`
	AlreadyTarget uint64                `json:"already_target"`
	State         string                `json:"state"`
	Intents       []writeIntent         `json:"intents"`
	Results       []writeObservedResult `json:"results,omitempty"`
}

func writeJournalPath(checkpointPath string) string {
	return checkpointPath + ".write-journal.json"
}

func writeJournalChecksum(journal writeJournal) (string, error) {
	journal.Checksum = ""
	body, err := json.Marshal(journal)
	if err != nil {
		return "", err
	}
	return sha256Hex(body), nil
}

func saveWriteJournal(path string, journal writeJournal) error {
	if journal.Schema != "" && journal.Schema != writeJournalSchema {
		return fmt.Errorf("unsupported write journal schema %q", journal.Schema)
	}
	journal.Schema = writeJournalSchema
	if err := validateWriteJournal(journal, journal.RunID, journal.ManifestHash, journal.Operation); err != nil {
		return err
	}
	if err := validateMutableStatePath(path, "write journal"); err != nil {
		return err
	}
	checksum, err := writeJournalChecksum(journal)
	if err != nil {
		return fmt.Errorf("hash write journal: %w", err)
	}
	journal.Checksum = checksum
	_, err = atomicJSON(path, journal)
	return err
}

func loadWriteJournal(path, runID, manifestHash, operation string) (writeJournal, bool, error) {
	body, found, err := readMutableStateFile(path, "write journal")
	if !found && err == nil {
		return writeJournal{}, false, nil
	}
	if err != nil {
		return writeJournal{}, false, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var journal writeJournal
	if err := decoder.Decode(&journal); err != nil {
		return writeJournal{}, false, fmt.Errorf("decode write journal: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return writeJournal{}, false, fmt.Errorf("write journal has trailing JSON")
	}
	if journal.Schema != writeJournalSchema {
		return writeJournal{}, false, fmt.Errorf("unsupported write journal schema %q", journal.Schema)
	}
	wantChecksum, err := writeJournalChecksum(journal)
	if err != nil {
		return writeJournal{}, false, fmt.Errorf("hash write journal: %w", err)
	}
	if journal.Checksum == "" || journal.Checksum != wantChecksum {
		return writeJournal{}, false, fmt.Errorf("write journal checksum mismatch")
	}
	if err := validateWriteJournal(journal, runID, manifestHash, operation); err != nil {
		return writeJournal{}, false, err
	}
	return journal, true, nil
}

func validateWriteJournal(journal writeJournal, runID, manifestHash, operation string) error {
	if journal.Schema != writeJournalSchema {
		return fmt.Errorf("unsupported write journal schema %q", journal.Schema)
	}
	if runID == "" || manifestHash == "" || (operation != applyMode && operation != rollbackMode) ||
		journal.RunID != runID || journal.ManifestHash != manifestHash || journal.Operation != operation {
		return fmt.Errorf("write journal does not match run/manifest/operation")
	}
	if journal.Start == 0 || journal.End < journal.Start || journal.BatchRecords != journal.End-journal.Start+1 ||
		journal.BatchBytes <= 0 || journal.AlreadyTarget > journal.BatchRecords ||
		uint64(len(journal.Intents)) != journal.BatchRecords-journal.AlreadyTarget ||
		(journal.State != writeJournalIssued && journal.State != writeJournalObserved) || len(journal.Intents) == 0 {
		return fmt.Errorf("write journal range, state, or intents are invalid")
	}
	previousOrdinal := uint64(0)
	for _, intent := range journal.Intents {
		if intent.Operation != operation || intent.Ordinal < journal.Start || intent.Ordinal > journal.End ||
			intent.Ordinal <= previousOrdinal || intent.Height == 0 || intent.Key == "" || intent.IfMatch == "" {
			return fmt.Errorf("write journal has an invalid intent")
		}
		if _, err := hex.DecodeString(intent.OldSHA256); err != nil || len(intent.OldSHA256) != 64 {
			return fmt.Errorf("write journal intent %d has an invalid old SHA-256", intent.Ordinal)
		}
		if _, err := hex.DecodeString(intent.NewSHA256); err != nil || len(intent.NewSHA256) != 64 {
			return fmt.Errorf("write journal intent %d has an invalid new SHA-256", intent.Ordinal)
		}
		if intent.ConfirmedPUTs > 1 || intent.UncertainPUTs > 1 ||
			intent.ConfirmedPUTs > intent.PUTAttempts || intent.UncertainPUTs > intent.PUTAttempts-intent.ConfirmedPUTs ||
			intent.Conflicts > intent.PUTAttempts ||
			intent.ConfirmedPUTs+intent.UncertainPUTs > 1 {
			return fmt.Errorf("write journal intent %d has inconsistent PUT counters", intent.Ordinal)
		}
		previousOrdinal = intent.Ordinal
	}
	intentsByOrdinal := make(map[uint64]writeIntent, len(journal.Intents))
	for _, intent := range journal.Intents {
		intentsByOrdinal[intent.Ordinal] = intent
	}
	seenResults := make(map[uint64]struct{}, len(journal.Results))
	for _, result := range journal.Results {
		if result.Ordinal < journal.Start || result.Ordinal > journal.End || result.PostPUTETag == "" ||
			(result.Outcome != writeOutcomeConfirmed && result.Outcome != writeOutcomeUncertain && result.Outcome != writeOutcomeReconciled) {
			return fmt.Errorf("write journal has an invalid observed result")
		}
		if result.Outcome == writeOutcomeReconciled && operation != applyMode {
			return fmt.Errorf("write journal has an invalid reconciled result for %s", operation)
		}
		intent, found := intentsByOrdinal[result.Ordinal]
		if !found {
			return fmt.Errorf("write journal result %d has no matching intent", result.Ordinal)
		}
		expectedSHA := intent.NewSHA256
		if operation == rollbackMode {
			expectedSHA = intent.OldSHA256
		}
		if result.ObservedSHA256 != expectedSHA {
			return fmt.Errorf("write journal result %d observed the wrong target", result.Ordinal)
		}
		if (result.Outcome == writeOutcomeConfirmed && intent.ConfirmedPUTs == 0) ||
			(result.Outcome == writeOutcomeUncertain && intent.UncertainPUTs == 0) ||
			(result.Outcome == writeOutcomeReconciled && (intent.PUTAttempts == 0 || intent.Conflicts == 0 || intent.ConfirmedPUTs != 0 || intent.UncertainPUTs != 0)) {
			return fmt.Errorf("write journal result %d has no matching PUT counter", result.Ordinal)
		}
		if _, found := seenResults[result.Ordinal]; found {
			return fmt.Errorf("write journal has duplicate result ordinal %d", result.Ordinal)
		}
		if _, err := hex.DecodeString(result.ObservedSHA256); err != nil || len(result.ObservedSHA256) != 64 {
			return fmt.Errorf("write journal result %d has an invalid SHA-256", result.Ordinal)
		}
		seenResults[result.Ordinal] = struct{}{}
	}
	if journal.State == writeJournalObserved && len(journal.Results) != len(journal.Intents) {
		return fmt.Errorf("observed write journal has incomplete results")
	}
	return nil
}

func removeWriteJournal(path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return syncDir(filepath.Dir(path))
}
