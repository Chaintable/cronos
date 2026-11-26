//go:build replay
// +build replay

// Package opendb opens a stopped node's RocksDB databases read-only. It calls
// the RocksDB C API directly: the archive is written by RocksDB 10, whose C
// API no longer has functions the pinned gorocksdb binds.
package opendb

/*
#cgo LDFLAGS: -lrocksdb
#include <stdlib.h>
#include <rocksdb/c.h>
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"unsafe"

	dbm "github.com/tendermint/tm-db"
)

var errReadOnly = errors.New("read-only database")

// OpenReadOnly opens dir/name.db read-only with its own block cache. RocksDB
// rejects every write; maxOpenFiles -1 keeps all table files open.
func OpenReadOnly(dir, name string, cacheBytes uint64, maxOpenFiles int) (dbm.DB, error) {
	table := C.rocksdb_block_based_options_create()
	defer C.rocksdb_block_based_options_destroy(table)
	cache := C.rocksdb_cache_create_lru(C.size_t(cacheBytes))
	C.rocksdb_block_based_options_set_block_cache(table, cache)
	opts := C.rocksdb_options_create()
	defer C.rocksdb_options_destroy(opts)
	C.rocksdb_options_set_block_based_table_factory(opts, table)
	C.rocksdb_options_set_max_open_files(opts, C.int(maxOpenFiles))

	path := C.CString(filepath.Join(dir, name+".db"))
	defer C.free(unsafe.Pointer(path))
	var cerr *C.char
	db := C.rocksdb_open_for_read_only(opts, path, 0, &cerr)
	if err := takeError(cerr); err != nil {
		C.rocksdb_cache_destroy(cache)
		return nil, fmt.Errorf("open %s.db: %w", name, err)
	}
	return &readOnlyDB{db: db, ro: C.rocksdb_readoptions_create(), cache: cache}, nil
}

func takeError(cerr *C.char) error {
	if cerr == nil {
		return nil
	}
	defer C.rocksdb_free(unsafe.Pointer(cerr))
	return errors.New(C.GoString(cerr))
}

func cKey(key []byte) (*C.char, C.size_t) {
	if len(key) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key))
}

type readOnlyDB struct {
	db    *C.rocksdb_t
	ro    *C.rocksdb_readoptions_t
	cache *C.rocksdb_cache_t
}

var _ dbm.DB = (*readOnlyDB)(nil)

func (d *readOnlyDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("key empty")
	}
	k, n := cKey(key)
	var size C.size_t
	var cerr *C.char
	v := C.rocksdb_get(d.db, d.ro, k, n, &size, &cerr)
	if err := takeError(cerr); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	defer C.rocksdb_free(unsafe.Pointer(v))
	return C.GoBytes(unsafe.Pointer(v), C.int(size)), nil
}

func (d *readOnlyDB) Has(key []byte) (bool, error) {
	v, err := d.Get(key)
	return v != nil, err
}

func (d *readOnlyDB) Set([]byte, []byte) error     { return errReadOnly }
func (d *readOnlyDB) SetSync([]byte, []byte) error { return errReadOnly }
func (d *readOnlyDB) Delete([]byte) error          { return errReadOnly }
func (d *readOnlyDB) DeleteSync([]byte) error      { return errReadOnly }
func (d *readOnlyDB) NewBatch() dbm.Batch          { return readOnlyBatch{} }
func (d *readOnlyDB) Print() error                 { return nil }
func (d *readOnlyDB) Stats() map[string]string     { return nil }

func (d *readOnlyDB) Close() error {
	C.rocksdb_readoptions_destroy(d.ro)
	C.rocksdb_close(d.db)
	C.rocksdb_cache_destroy(d.cache)
	return nil
}

func (d *readOnlyDB) Iterator(start, end []byte) (dbm.Iterator, error) {
	return newIterator(C.rocksdb_create_iterator(d.db, d.ro), start, end, false), nil
}

func (d *readOnlyDB) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	return newIterator(C.rocksdb_create_iterator(d.db, d.ro), start, end, true), nil
}

type readOnlyBatch struct{}

func (readOnlyBatch) Set([]byte, []byte) error { return errReadOnly }
func (readOnlyBatch) Delete([]byte) error      { return errReadOnly }
func (readOnlyBatch) Write() error             { return errReadOnly }
func (readOnlyBatch) WriteSync() error         { return errReadOnly }
func (readOnlyBatch) Close() error             { return nil }

// iterator follows tm-db's rocksDBIterator semantics.
type iterator struct {
	source     *C.rocksdb_iterator_t
	start, end []byte
	reverse    bool
	invalid    bool
}

func newIterator(source *C.rocksdb_iterator_t, start, end []byte, reverse bool) *iterator {
	it := &iterator{source: source, start: start, end: end, reverse: reverse}
	switch {
	case reverse && end == nil:
		C.rocksdb_iter_seek_to_last(source)
	case reverse:
		k, n := cKey(end)
		C.rocksdb_iter_seek(source, k, n)
		if C.rocksdb_iter_valid(source) != 0 {
			if bytes.Compare(end, it.rawKey()) <= 0 {
				C.rocksdb_iter_prev(source)
			}
		} else {
			C.rocksdb_iter_seek_to_last(source)
		}
	case start == nil:
		C.rocksdb_iter_seek_to_first(source)
	default:
		k, n := cKey(start)
		C.rocksdb_iter_seek(source, k, n)
	}
	return it
}

func (it *iterator) rawKey() []byte {
	var n C.size_t
	k := C.rocksdb_iter_key(it.source, &n)
	return C.GoBytes(unsafe.Pointer(k), C.int(n))
}

func (it *iterator) Domain() ([]byte, []byte) { return it.start, it.end }

func (it *iterator) Valid() bool {
	if it.invalid {
		return false
	}
	if it.Error() != nil || C.rocksdb_iter_valid(it.source) == 0 {
		it.invalid = true
		return false
	}
	key := it.rawKey()
	if it.reverse && it.start != nil && bytes.Compare(key, it.start) < 0 ||
		!it.reverse && it.end != nil && bytes.Compare(it.end, key) <= 0 {
		it.invalid = true
		return false
	}
	return true
}

func (it *iterator) Key() []byte {
	it.assertValid()
	return it.rawKey()
}

func (it *iterator) Value() []byte {
	it.assertValid()
	var n C.size_t
	v := C.rocksdb_iter_value(it.source, &n)
	return C.GoBytes(unsafe.Pointer(v), C.int(n))
}

func (it *iterator) Next() {
	it.assertValid()
	if it.reverse {
		C.rocksdb_iter_prev(it.source)
	} else {
		C.rocksdb_iter_next(it.source)
	}
}

func (it *iterator) Error() error {
	var cerr *C.char
	C.rocksdb_iter_get_error(it.source, &cerr)
	return takeError(cerr)
}

func (it *iterator) Close() error {
	C.rocksdb_iter_destroy(it.source)
	return nil
}

func (it *iterator) assertValid() {
	if !it.Valid() {
		panic("iterator is invalid")
	}
}
