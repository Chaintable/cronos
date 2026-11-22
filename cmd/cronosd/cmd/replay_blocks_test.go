package cmd

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// H5542610 runs an IBC proof check that writes a diagnostic to process stdout
// in the historical ICS23 dependency. RPC consumes the worker's entire stdout.
func TestReplayIBCBlockJSON(t *testing.T) {
	archive := os.Getenv("REPLAY_V083_TEST_ARCHIVE")
	if archive == "" {
		t.Skip("requires a sealed application database with mainnet H5542609")
	}
	worker := exec.Command(os.Getenv("REPLAY_BINARY"), "replay-blocks",
		os.Getenv("REPLAY_V083_TEST_RPC"), "5542610", "5542610",
		"--home", t.TempDir(), "--archive-state-dir", archive)
	var stderr bytes.Buffer
	worker.Stderr = &stderr
	raw, err := worker.Output()
	if err != nil {
		t.Fatalf("replay failed: %v: %s", err, stderr.Bytes())
	}
	if err := checkBundle(raw, 5542610, common.HexToHash("0x4519fa9ed144023ea1931a8432efe3ec586131a3c99328b55c3c90117aeafbaa")); err != nil {
		t.Fatal(err)
	}
}
