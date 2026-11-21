//go:build replay_rocksdb || rocksdb
// +build replay_rocksdb rocksdb

package cmd

/*
#cgo LDFLAGS: -lrocksdb
#include <stdlib.h>
#include <rocksdb/c.h>
*/
import "C"
import (
	"errors"
	"unsafe"

	"github.com/evmos/ethermint/debank/replay"
)

type replayRocks struct {
	db   *C.rocksdb_t
	read *C.rocksdb_readoptions_t
}

func rocksError(msg *C.char) error {
	defer C.rocksdb_free(unsafe.Pointer(msg))
	return errors.New(C.GoString(msg))
}
func init() {
	openReplayRocks = func(path string) (replay.ArchiveReader, func(), error) {
		name := C.CString(path)
		defer C.free(unsafe.Pointer(name))
		options := C.rocksdb_options_create()
		defer C.rocksdb_options_destroy(options)
		// IAVL follows hash keys across SSTs. Cache partitioned indexes and
		// consult the archive's on-disk filters before reading data blocks.
		table := C.rocksdb_block_based_options_create()
		defer C.rocksdb_block_based_options_destroy(table)
		cache := C.rocksdb_cache_create_lru(128 << 20)
		defer C.rocksdb_cache_destroy(cache)
		C.rocksdb_block_based_options_set_block_cache(table, cache)
		C.rocksdb_block_based_options_set_cache_index_and_filter_blocks(table, 1)
		C.rocksdb_block_based_options_set_filter_policy(table, C.rocksdb_filterpolicy_create_bloom_full(10))
		C.rocksdb_options_set_block_based_table_factory(options, table)
		C.rocksdb_options_set_max_open_files(options, 512)
		var msg *C.char
		db := C.rocksdb_open_for_read_only(options, name, 0, &msg)
		if msg != nil {
			return nil, nil, rocksError(msg)
		}
		read := C.rocksdb_readoptions_create()
		return &replayRocks{db: db, read: read}, func() { C.rocksdb_readoptions_destroy(read); C.rocksdb_close(db) }, nil
	}
}
func (r *replayRocks) Get(key []byte) ([]byte, error) {
	var msg *C.char
	var size C.size_t
	raw := C.rocksdb_get(r.db, r.read, (*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)), &size, &msg)
	if msg != nil {
		return nil, rocksError(msg)
	}
	if raw == nil {
		return nil, nil
	}
	defer C.rocksdb_free(unsafe.Pointer(raw))
	return C.GoBytes(unsafe.Pointer(raw), C.int(size)), nil
}
