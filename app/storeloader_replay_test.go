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

// The archive's latest version lacks a store this release still mounts, as
// with capability and feeibc after the v1.5 upgrade.
func TestReplayHostStoreLoaderMountsDeletedStoresEmpty(t *testing.T) {
	db := dbm.NewMemDB()
	kept := storetypes.NewKVStoreKey("kept")
	archive := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
	archive.MountStoreWithDB(kept, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, archive.LoadLatestVersion())
	archive.GetCommitKVStore(kept).Set([]byte("k"), []byte("v"))
	want := archive.Commit()

	open := func() *rootmulti.Store {
		rs := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
		rs.MountStoreWithDB(kept, storetypes.StoreTypeIAVL, nil)
		rs.MountStoreWithDB(storetypes.NewKVStoreKey("deleted"), storetypes.StoreTypeIAVL, nil)
		rs.MountStoreWithDB(storetypes.NewTransientStoreKey("transient"), storetypes.StoreTypeTransient, nil)
		return rs
	}
	require.ErrorContains(t, open().LoadLatestVersion(), "version of store deleted mismatch")

	rs := open()
	require.NoError(t, ReplayHostStoreLoader(rs))
	require.Equal(t, want, rs.LastCommitID())
	require.Equal(t, []byte("v"), rs.GetCommitKVStore(kept).Get([]byte("k")))
}
