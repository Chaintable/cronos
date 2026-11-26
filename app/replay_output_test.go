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

func TestCheckReplayHeight(t *testing.T) {
	app := &App{}
	for _, h := range []int64{45062800, 58825799} {
		require.NoError(t, app.CheckReplayHeight("cronosmainnet_25-1", h))
	}
	for _, h := range []int64{45062799, 58825800} {
		require.Error(t, app.CheckReplayHeight("cronosmainnet_25-1", h))
	}
	require.Error(t, app.CheckReplayHeight("cronos_777-1", 45062800))
}
