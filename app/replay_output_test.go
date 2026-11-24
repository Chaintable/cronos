package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockFileOnly(t *testing.T) {
	for value, want := range map[string]bool{"": false, "full": false, "blockfile": true} {
		t.Setenv("CRONOS_REPLAY_OUTPUT", value)
		got, err := (&App{}).BlockFileOnly()
		require.NoError(t, err, value)
		require.Equal(t, want, got, value)
	}
	t.Setenv("CRONOS_REPLAY_OUTPUT", "state")
	_, err := (&App{}).BlockFileOnly()
	require.ErrorContains(t, err, "invalid CRONOS_REPLAY_OUTPUT")
}
