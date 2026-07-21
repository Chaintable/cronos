package main

import (
	"fmt"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	storetypes "cosmossdk.io/store/types"
	dbm "github.com/cosmos/cosmos-db"

	"github.com/crypto-org-chain/cronos/cmd/cronosd/opendb"
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
	db, err := opendb.OpenReadOnlyDB(home, dbm.RocksDBBackend)
	if err != nil {
		return nil, err
	}
	return &archiveReader{home: home, db: db, store: rootmulti.NewStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())}, nil
}

func (a *archiveReader) Close() error { return a.db.Close() }

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
	latest := rootmulti.GetLatestVersion(a.db)
	info, err := a.commitInfo(latest)
	if err != nil {
		return archiveIdentity{}, err
	}
	return archiveIdentity{Home: a.home, LatestVersion: latest, FinalCommitHash: commonHash(info.Hash())}, nil
}

func commonHash(body []byte) string {
	return fmt.Sprintf("0x%x", body)
}
