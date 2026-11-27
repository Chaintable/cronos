//go:build replay
// +build replay

package cmd

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math"

	"github.com/VictoriaMetrics/fastcache"
	dbm "github.com/cometbft/cometbft-db"
)

// replayNodeCache sits below all request-local overlays on the shared, read-only
// application database. Each hit returns a fresh copy of the encoded IAVL node.
type replayNodeCache struct {
	dbm.DB
	cache *fastcache.Cache
}

func newReplayNodeCache(db dbm.DB, sizeMB int) (*replayNodeCache, error) {
	// fastcache splits storage into 512 buckets of 64 KiB chunks and requires
	// each bucket to stay below 1 TiB. Round down to respect the data budget.
	if sizeMB < 32 || sizeMB >= 1<<29 || sizeMB > math.MaxInt>>20 {
		return nil, fmt.Errorf("invalid CRONOS_REPLAY_NODE_CACHE_MB=%d (expected 0 or a supported capacity of at least 32 MiB)", sizeMB)
	}
	return &replayNodeCache{DB: db, cache: fastcache.New((sizeMB / 32 * 32) << 20)}, nil
}

func isReplayNodeKey(key []byte) bool {
	const storePrefix = "s/k:"
	// rootmulti prefixes the IAVL key n<32-byte hash> with s/k:<store>/.
	end := len(key) - sha256.Size - 2
	return end > len(storePrefix) && bytes.HasPrefix(key, []byte(storePrefix)) &&
		key[end] == '/' && key[end+1] == 'n' && bytes.IndexByte(key[len(storePrefix):end], '/') == -1
}

func (db *replayNodeCache) Get(key []byte) ([]byte, error) {
	if !isReplayNodeKey(key) {
		return db.DB.Get(key)
	}
	if value := db.cache.Get(nil, key); value != nil {
		return value, nil
	}
	value, err := db.DB.Get(key)
	if err == nil && len(value) > 0 {
		// Set copies both key and value. Entries too large for a chunk are ignored.
		db.cache.Set(key, value)
	}
	return value, err
}

func (db *replayNodeCache) Has(key []byte) (bool, error) {
	if isReplayNodeKey(key) && db.cache.Has(key) {
		return true, nil
	}
	return db.DB.Has(key)
}

func (db *replayNodeCache) report(w io.Writer) {
	var stats fastcache.Stats
	db.cache.UpdateStats(&stats)
	fmt.Fprintf(w, "replay worker timing node_cache hits=%d misses=%d entries=%d data_bytes=%d\n",
		stats.GetCalls-stats.Misses, stats.Misses, stats.EntriesCount, stats.BytesSize)
}
