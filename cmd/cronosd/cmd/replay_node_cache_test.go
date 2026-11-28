//go:build replay
// +build replay

package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	dbm "github.com/cometbft/cometbft-db"
)

type nodeCacheTestDB struct {
	dbm.DB
	gets, checks int
	err          error
}

func (d *nodeCacheTestDB) Get(key []byte) ([]byte, error) {
	d.gets++
	if d.err != nil {
		return nil, d.err
	}
	return d.DB.Get(key)
}

func (d *nodeCacheTestDB) Has(key []byte) (bool, error) {
	d.checks++
	if d.err != nil {
		return false, d.err
	}
	return d.DB.Has(key)
}

func TestReplayNodeCache(t *testing.T) {
	parent := &nodeCacheTestDB{DB: dbm.NewMemDB()}
	db, err := newReplayNodeCache(parent, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer db.cache.Reset()
	hash := bytes.Repeat([]byte{'/'}, sha256.Size)
	left := append([]byte("s/k:evm/n"), hash...)
	right := append([]byte("s/k:bank/n"), hash...)
	for name, key := range map[string][]byte{"evm": left, "bank": right} {
		source := []byte(name)
		if err := parent.Set(key, source); err != nil {
			t.Fatal(err)
		}
		value, err := db.Get(key)
		if string(value) != name || err != nil {
			t.Fatalf("cold lookup %s: %q, %v", name, value, err)
		}
		value[0] = '!'
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name, key := range map[string][]byte{"evm": left, "bank": right} {
				value, err := db.Get(key)
				if string(value) != name || err != nil {
					t.Errorf("warm lookup %s: %q, %v", name, value, err)
					return
				}
				value[0] = '?'
				if ok, err := db.Has(key); !ok || err != nil {
					t.Errorf("cached Has(%s): %v, %v", name, ok, err)
				}
			}
		}()
	}
	wg.Wait()
	if parent.gets != 2 || parent.checks != 0 {
		t.Fatalf("warm lookups reached parent: Get=%d Has=%d", parent.gets, parent.checks)
	}
}

func TestReplayNodeCachePassThrough(t *testing.T) {
	parent := &nodeCacheTestDB{DB: dbm.NewMemDB()}
	db, err := newReplayNodeCache(parent, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer db.cache.Reset()
	hash := make([]byte, sha256.Size)
	key := append([]byte("s/k:evm/n"), hash...)
	wantErr := errors.New("archive unavailable")
	parent.err = wantErr
	if _, err := db.Get(key); err != wantErr {
		t.Fatalf("Get error: %v", err)
	}
	if _, err := db.Has(key); err != wantErr {
		t.Fatalf("Has error: %v", err)
	}
	parent.err = nil
	if v, err := db.Get(key); v != nil || err != nil {
		t.Fatalf("missing node: %v, %v", v, err)
	}
	if err := parent.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(key); v == nil || len(v) != 0 || err != nil {
		t.Fatalf("empty node: %v, %v", v, err)
	}
	for _, other := range [][]byte{
		append([]byte("s/k:evm/r"), hash...),
		append([]byte("s/k:evm/fprefix/n"), hash...),
		append([]byte("s/k:/n"), hash...),
		key[:len(key)-1], append([]byte("outside/n"), hash...),
	} {
		if isReplayNodeKey(other) {
			t.Fatalf("non-node key selected: %x", other)
		}
	}
	large := bytes.Repeat([]byte("v"), 1<<16)
	if err := parent.Set(key, large); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get(key); !bytes.Equal(v, large) || err != nil {
		t.Fatalf("oversized node: length %d, error %v", len(v), err)
	}
	parent.err = wantErr
	if _, err := db.Get(key); err != wantErr {
		t.Fatalf("oversized node should pass through: %v", err)
	}
}

func TestReplayNodeCacheBudget(t *testing.T) {
	parent := dbm.NewMemDB()
	if _, err := newReplayNodeCache(parent, 1); err == nil {
		t.Fatal("accepted less than the minimum cache budget")
	}
	db, err := newReplayNodeCache(parent, 33)
	if err != nil {
		t.Fatal(err)
	}
	defer db.cache.Reset()
	// Fill the cache beyond its budget with values that each need their own chunk.
	value := make([]byte, 48<<10)
	key := make([]byte, 8)
	for i := 0; i < 4096; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		db.cache.Set(key, value)
	}
	var stats fastcache.Stats
	db.cache.UpdateStats(&stats)
	if stats.BytesSize == 0 || stats.BytesSize > 32<<20 {
		t.Fatalf("expected data to fit within the rounded budget, got %d bytes", stats.BytesSize)
	}
}
