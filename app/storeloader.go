package app

import (
	"fmt"
	"os"
	"sort"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	storetypes "cosmossdk.io/store/types"
	upgradetypes "cosmossdk.io/x/upgrade/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/baseapp"
)

// MaxVersionStoreLoader will be used when there's versiondb to cap the loaded iavl version
func MaxVersionStoreLoader(version int64) baseapp.StoreLoader {
	if version == 0 {
		return baseapp.DefaultStoreLoader
	}

	return func(ms storetypes.CommitMultiStore) error {
		return ms.LoadVersion(version)
	}
}

// MaxVersionUpgradeStoreLoader is used to prepare baseapp with a fixed StoreLoader
func MaxVersionUpgradeStoreLoader(version int64, upgradeHeight int64, storeUpgrades *storetypes.StoreUpgrades) baseapp.StoreLoader {
	if version == 0 {
		return upgradetypes.UpgradeStoreLoader(upgradeHeight, storeUpgrades)
	}

	return func(ms storetypes.CommitMultiStore) error {
		if upgradeHeight == ms.LastCommitID().Version+1 {
			// Check if the current commit version and upgrade height matches
			if len(storeUpgrades.Renamed) > 0 || len(storeUpgrades.Deleted) > 0 || len(storeUpgrades.Added) > 0 {
				return ms.LoadLatestVersionAndUpgrade(storeUpgrades)
			}
		}

		// Otherwise load default store loader
		return MaxVersionStoreLoader(version)(ms)
	}
}

// replayHostMissingStores lists the persistent stores absent from the latest
// commit when a replay host (CRONOS_REPLAY_CATCH_UP=false) opens an archive
// written by a later release, e.g. capability and feeibc deleted by v1.5.
// Such a host never commits; replays load their parents separately.
func replayHostMissingStores(db dbm.DB, keys map[string]*storetypes.KVStoreKey) []string {
	if os.Getenv("CRONOS_REPLAY_CATCH_UP") != "false" {
		return nil
	}
	version := rootmulti.GetLatestVersion(db)
	if version == 0 {
		return nil
	}
	info, err := rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics()).GetCommitInfo(version)
	if err != nil {
		panic(fmt.Sprintf("replay host commit info at %d: %s", version, err))
	}
	committed := make(map[string]bool, len(info.StoreInfos))
	for _, si := range info.StoreInfos {
		committed[si.Name] = true
	}
	var missing []string
	for name := range keys {
		if !committed[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	return missing
}
