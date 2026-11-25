package app

import (
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	storetypes "cosmossdk.io/store/types"
	dbm "github.com/cosmos/cosmos-db"
	icahosttypes "github.com/cosmos/ibc-go/v8/modules/apps/27-interchain-accounts/host/types"
	"github.com/stretchr/testify/require"
)

// At the v1.4 upgrade block the parent version lacks the icahost store,
// while the archive already holds its later versions. Replaying the upgrade
// block must start the store empty at the upgrade height, as mainnet did.
func TestUpgradeBlockStartsAddedStoreEmpty(t *testing.T) {
	db := dbm.NewMemDB()
	acc := storetypes.NewKVStoreKey("acc")
	icahost := storetypes.NewKVStoreKey(icahosttypes.StoreKey)
	newStore := func(db dbm.DB) *rootmulti.Store {
		cms := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
		cms.SetIAVLDisableFastNode(true)
		return cms
	}
	old := newStore(db)
	old.MountStoreWithDB(acc, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, old.LoadLatestVersion())
	old.GetKVStore(acc).Set([]byte("k"), []byte("v"))
	parent := old.Commit().Version

	// The node upgrades at parent+1 and keeps running.
	node := newStore(db)
	node.MountStoreWithDB(acc, storetypes.StoreTypeIAVL, nil)
	node.MountStoreWithDB(icahost, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, node.LoadVersionAndUpgrade(parent, activationStoreUpgrades()))
	node.GetKVStore(icahost).Set([]byte("params"), []byte{1})
	upgraded := node.Commit()
	node.GetKVStore(icahost).Set([]byte("params"), []byte{2})
	node.Commit()

	replay := func(icahostDB dbm.DB) []byte {
		cms := newStore(db)
		cms.MountStoreWithDB(acc, storetypes.StoreTypeIAVL, nil)
		cms.MountStoreWithDB(icahost, storetypes.StoreTypeIAVL, icahostDB)
		require.NoError(t, cms.LoadVersionAndUpgrade(parent, activationStoreUpgrades()))
		cms.GetKVStore(icahost).Set([]byte("params"), []byte{1})
		return cms.WorkingHash()
	}
	require.Equal(t, upgraded.Hash, replay(dbm.NewMemDB()))
	require.NotEqual(t, upgraded.Hash, replay(nil), "the archive holds the store's later versions")
}
