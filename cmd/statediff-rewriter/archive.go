package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl"
	iavldb "github.com/cosmos/iavl/db"
	"github.com/crypto-org-chain/cronos/cmd/cronosd/opendb"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	storetypes "cosmossdk.io/store/types"
	"cosmossdk.io/store/wrapper"
)

type archiveReader struct {
	home  string
	db    dbm.DB
	store *rootmulti.Store
}

func openArchive(home string) (*archiveReader, error) {
	if !rocksDBBuild {
		return nil, fmt.Errorf("statediff-rewriter requires build tags rocksdb and grocksdb_clean_link")
	}
	databasePath := filepath.Join(home, "data", "application.db")
	if _, err := requireReadOnlyFilesystem(databasePath, "archive database"); err != nil {
		return nil, err
	}
	db, err := opendb.OpenReadOnlyDB(home, dbm.RocksDBBackend)
	if err != nil {
		return nil, err
	}
	return &archiveReader{home: home, db: db, store: rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())}, nil
}

func (a *archiveReader) Close() error { return a.db.Close() }

func (a *archiveReader) iavlDB(storeName string) iavldb.DB {
	return wrapper.NewDBWrapper(dbm.NewPrefixDB(a.db, []byte(iavlStorePrefix(storeName))))
}

func iavlStorePrefix(storeName string) string {
	return "s/k:" + storeName + "/"
}

func (a *archiveReader) evmIAVLDB() iavldb.DB {
	return a.iavlDB("evm")
}

func (a *archiveReader) stateChangeSource(storeName string, cacheSize int) keyRangeStateChangeSource {
	return iavl.NewImmutableTree(a.iavlDB(storeName), cacheSize, true, log.NewNopLogger())
}

func (a *archiveReader) versionedStateSource(storeName string, cacheSize int) *iavl.MutableTree {
	return iavl.NewMutableTree(a.iavlDB(storeName), cacheSize, true, log.NewNopLogger())
}

func (a *archiveReader) evmStateChangeSource(cacheSize int) keyRangeStateChangeSource {
	return a.stateChangeSource("evm", cacheSize)
}

func (a *archiveReader) legacyLatestVersion(storeName string) (int64, error) {
	return legacyLatestRootVersion(a.iavlDB(storeName))
}

func (a *archiveReader) evmLegacyLatestVersion() (int64, error) {
	return a.legacyLatestVersion("evm")
}

func (a *archiveReader) commitInfo(version int64) (*storetypes.CommitInfo, error) {
	info, err := a.store.GetCommitInfo(version)
	if err != nil {
		return nil, fmt.Errorf("read commit info %d: %w", version, err)
	}
	if info.Version != version {
		return nil, fmt.Errorf("commit info %d has version %d", version, info.Version)
	}
	return info, nil
}

func (a *archiveReader) identity() (archiveIdentity, error) {
	databaseIdentity, err := readRocksDBIdentity(a.home)
	if err != nil {
		return archiveIdentity{}, err
	}
	latest := rootmulti.GetLatestVersion(a.db)
	info, err := a.commitInfo(latest)
	if err != nil {
		return archiveIdentity{}, err
	}
	return archiveIdentity{
		Home: a.home, DatabaseIdentity: databaseIdentity,
		LatestVersion: latest, FinalCommitHash: commonHash(info.Hash()),
	}, nil
}

func readRocksDBIdentity(home string) (string, error) {
	path := filepath.Join(home, "data", "application.db", "IDENTITY")
	body, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read RocksDB identity %s: %w", path, err)
	}
	fields := strings.Fields(string(body))
	if len(fields) != 1 {
		return "", fmt.Errorf("RocksDB identity %s must contain exactly one value", path)
	}
	return fields[0], nil
}

func commonHash(body []byte) string {
	return fmt.Sprintf("0x%x", body)
}
