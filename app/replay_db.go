package app

import (
	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/dbadapter"
	dbm "github.com/cosmos/cosmos-db"
)

// overlayDB buffers every write in memory over a read-only parent, so a
// segment can commit a replayed block natively without touching the archive.
// The SDK cache store merges buffered writes and deletes into reads and
// iterators.
type overlayDB struct {
	cache *cachekv.Store
}

var _ dbm.DB = (*overlayDB)(nil)

func newOverlayDB(parent dbm.DB) *overlayDB {
	return &overlayDB{cache: cachekv.NewStore(dbadapter.Store{DB: parent})}
}

func (o *overlayDB) Get(key []byte) ([]byte, error) { return o.cache.Get(key), nil }
func (o *overlayDB) Has(key []byte) (bool, error)   { return o.cache.Has(key), nil }

func (o *overlayDB) Set(key, value []byte) error {
	o.cache.Set(key, value)
	return nil
}

func (o *overlayDB) SetSync(key, value []byte) error { return o.Set(key, value) }

func (o *overlayDB) Delete(key []byte) error {
	o.cache.Delete(key)
	return nil
}

func (o *overlayDB) DeleteSync(key []byte) error { return o.Delete(key) }

func (o *overlayDB) Iterator(start, end []byte) (dbm.Iterator, error) {
	return overlayIterator{o.cache.Iterator(start, end)}, nil
}

func (o *overlayDB) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	return overlayIterator{o.cache.ReverseIterator(start, end)}, nil
}

// overlayIterator reports no error once exhausted, as database iterators do;
// the cache store's merge iterator returns an error then.
type overlayIterator struct {
	dbm.Iterator
}

func (overlayIterator) Error() error { return nil }

// Close leaves the shared parent open.
func (o *overlayDB) Close() error                   { return nil }
func (o *overlayDB) Print() error                   { return nil }
func (o *overlayDB) Stats() map[string]string       { return nil }
func (o *overlayDB) NewBatch() dbm.Batch            { return &overlayBatch{db: o} }
func (o *overlayDB) NewBatchWithSize(int) dbm.Batch { return o.NewBatch() }

type overlayOp struct {
	key, value []byte
	delete     bool
}

type overlayBatch struct {
	db   *overlayDB
	ops  []overlayOp
	size int
}

func (b *overlayBatch) Set(key, value []byte) error {
	b.ops = append(b.ops, overlayOp{key: append([]byte{}, key...), value: append([]byte{}, value...)})
	b.size += len(key) + len(value)
	return nil
}

func (b *overlayBatch) Delete(key []byte) error {
	b.ops = append(b.ops, overlayOp{key: append([]byte{}, key...), delete: true})
	b.size += len(key)
	return nil
}

func (b *overlayBatch) Write() error {
	for _, op := range b.ops {
		if op.delete {
			b.db.cache.Delete(op.key)
		} else {
			b.db.cache.Set(op.key, op.value)
		}
	}
	b.ops, b.size = nil, 0
	return nil
}

func (b *overlayBatch) WriteSync() error          { return b.Write() }
func (b *overlayBatch) Close() error              { return nil }
func (b *overlayBatch) GetByteSize() (int, error) { return b.size, nil }
