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

func TestReplayUpgradeDeletesLegacyStores(t *testing.T) {
	archive := dbm.NewMemDB()
	cms := rootmulti.NewStore(archive, log.NewNopLogger(), metrics.NewNoOpMetrics())
	cms.SetIAVLDisableFastNode(true)
	acc := storetypes.NewKVStoreKey("acc")
	for _, name := range []string{"capability", "feeibc"} {
		cms.MountStoreWithDB(storetypes.NewKVStoreKey(name), storetypes.StoreTypeIAVL, nil)
	}
	cms.MountStoreWithDB(acc, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, cms.LoadLatestVersion())
	cms.GetKVStore(acc).Set([]byte("key"), []byte("value"))
	parent := cms.Commit()
	upgraded := rootmulti.NewStore(newOverlayDB(archive), log.NewNopLogger(), metrics.NewNoOpMetrics())
	upgraded.SetIAVLDisableFastNode(true)
	upgraded.MountStoreWithDB(acc, storetypes.StoreTypeIAVL, nil)
	require.NoError(t, upgraded.LoadVersionAndUpgrade(parent.Version, activationStoreUpgrades()))
	require.Equal(t, parent, upgraded.LastCommitID())
	upgraded.Commit()
	info, err := upgraded.GetCommitInfo(parent.Version + 1)
	require.NoError(t, err)
	require.Len(t, info.StoreInfos, 1)
	require.Equal(t, "acc", info.StoreInfos[0].Name)
	require.Equal(t, parent.Version, cms.LatestVersion())
}
