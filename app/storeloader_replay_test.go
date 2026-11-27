package app

import (
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	storetypes "cosmossdk.io/store/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"
)

// A later release deleted a store this release mounts, as v1.5 did with
// capability and feeibc; its old data stays under its prefix.
func TestReplayHostOpensArchiveWithDeletedStores(t *testing.T) {
	db := dbm.NewMemDB()
	kept, deleted := storetypes.NewKVStoreKey("kept"), storetypes.NewKVStoreKey("deleted")
	older := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
	older.MountStoreWithDB(kept, storetypes.StoreTypeIAVL, nil)
	older.MountStoreWithDB(deleted, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, older.LoadLatestVersion())
	older.GetCommitKVStore(deleted).Set([]byte("k"), []byte("old"))
	older.Commit()
	later := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
	later.MountStoreWithDB(kept, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, later.LoadLatestVersion())
	later.GetCommitKVStore(kept).Set([]byte("k"), []byte("v"))
	want := later.Commit()

	keys := map[string]*storetypes.KVStoreKey{"kept": kept, "deleted": deleted}
	require.Nil(t, replayHostMissingStores(db, keys), "only a replay host without catch-up")
	t.Setenv("CRONOS_REPLAY_CATCH_UP", "false")
	missing := replayHostMissingStores(db, keys)
	require.Equal(t, []string{"deleted"}, missing)

	open := func(deletedDB dbm.DB) (*rootmulti.Store, error) {
		rs := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
		rs.MountStoreWithDB(kept, storetypes.StoreTypeIAVL, nil)
		rs.MountStoreWithDB(deleted, storetypes.StoreTypeIAVL, deletedDB)
		return rs, rs.LoadLatestVersionAndUpgrade(&storetypes.StoreUpgrades{Added: missing})
	}
	_, err := open(nil)
	require.ErrorContains(t, err, "found earlier version")
	rs, err := open(dbm.NewMemDB())
	require.NoError(t, err)
	require.Equal(t, want, rs.LastCommitID())
	require.Equal(t, []byte("v"), rs.GetCommitKVStore(kept).Get([]byte("k")))
}
