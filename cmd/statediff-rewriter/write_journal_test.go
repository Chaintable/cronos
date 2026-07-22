package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testWriteJournal() writeJournal {
	return writeJournal{
		RunID: "run", ManifestHash: "manifest", Operation: "apply",
		Start: 1, End: 1, BatchRecords: 1, BatchBytes: 1, State: writeJournalIssued,
		Intents: []writeIntent{{
			Operation: "apply", Ordinal: 1, Height: 2, Key: "prefix/key", IfMatch: "etag-old",
			OldSHA256: "0101010101010101010101010101010101010101010101010101010101010101",
			NewSHA256: "0202020202020202020202020202020202020202020202020202020202020202",
		}},
	}
}

func TestWriteJournalRejectsMutationAndWrongObservedTarget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "write-journal.json")
	journal := testWriteJournal()
	require.NoError(t, saveWriteJournal(path, journal))
	loaded, found, err := loadWriteJournal(path, journal.RunID, journal.ManifestHash, journal.Operation)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, journal.Intents, loaded.Intents)

	body, err := os.ReadFile(path)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(body, &fields))
	fields["state"] = writeJournalObserved
	mutated, err := json.Marshal(fields)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, mutated, 0o600))
	_, _, err = loadWriteJournal(path, journal.RunID, journal.ManifestHash, journal.Operation)
	require.ErrorContains(t, err, "checksum mismatch")

	journal = testWriteJournal()
	require.NoError(t, saveWriteJournal(path, journal))
	journal.State = writeJournalObserved
	journal.Intents[0].PUTAttempts = 1
	journal.Intents[0].ConfirmedPUTs = 1
	journal.Results = []writeObservedResult{{
		Ordinal: 1, ObservedSHA256: journal.Intents[0].OldSHA256,
		PostPUTETag: "etag-new", Outcome: "confirmed",
	}}
	err = saveWriteJournal(path, journal)
	require.ErrorContains(t, err, "wrong target")
	loaded, found, err = loadWriteJournal(path, journal.RunID, journal.ManifestHash, journal.Operation)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, writeJournalIssued, loaded.State)
}
