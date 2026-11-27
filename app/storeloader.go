package app

import (
	"sort"

	"cosmossdk.io/store/rootmulti"
	storetypes "cosmossdk.io/store/types"
	upgradetypes "cosmossdk.io/x/upgrade/types"
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

// ReplayHostStoreLoader opens the latest version of an archive written by a
// later release: stores that a later upgrade deleted are mounted empty. A
// replay host never commits, so nothing is written.
func ReplayHostStoreLoader(ms storetypes.CommitMultiStore) error {
	rs, ok := ms.(*rootmulti.Store)
	version := ms.LastCommitID().Version
	if !ok || version == 0 {
		return baseapp.DefaultStoreLoader(ms)
	}
	info, err := rs.GetCommitInfo(version)
	if err != nil {
		return err
	}
	committed := make(map[string]bool, len(info.StoreInfos))
	for _, si := range info.StoreInfos {
		committed[si.Name] = true
	}
	var added []string
	for name, key := range rs.StoreKeysByName() {
		if _, persistent := key.(*storetypes.KVStoreKey); persistent && !committed[name] {
			added = append(added, name)
		}
	}
	sort.Strings(added)
	return ms.LoadLatestVersionAndUpgrade(&storetypes.StoreUpgrades{Added: added})
}
