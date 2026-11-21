package cmd

import (
	"fmt"
	"path/filepath"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func openReplayArchive(dir string) (replay.ArchiveReader, func(), error) {
	options, err := filepath.Glob(filepath.Join(dir, "application.db", "OPTIONS-*"))
	if err != nil {
		return nil, nil, err
	}
	if len(options) > 0 {
		if openReplayRocks == nil {
			return nil, nil, fmt.Errorf("reading RocksDB replay archives requires the replay_rocksdb build tag")
		}
		return openReplayRocks(filepath.Join(dir, "application.db"))
	}
	db, err := dbm.NewGoLevelDBWithOpts("application", dir, &opt.Options{ReadOnly: true, ErrorIfMissing: true})
	if err != nil {
		return nil, nil, err
	}
	return db, func() { db.Close() }, nil
}

var openReplayRocks func(string) (replay.ArchiveReader, func(), error)
