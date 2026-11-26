package app

import (
	"bytes"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cosmos/cosmos-sdk/store/cachekv"
	"github.com/cosmos/cosmos-sdk/store/dbadapter"
)

// overlayDB buffers every write in memory over a read-only parent, so a
// replayed block can be committed natively without touching the archive.
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

// overlayIterator reports no error once exhausted, as tm-db iterators do; the
// cache store's merge iterator returns an error then, which IAVL treats as
// fatal when it scans a store that is still empty.
type overlayIterator struct {
	dbm.Iterator
}

func (overlayIterator) Error() error { return nil }

// Close leaves the shared parent open.
func (o *overlayDB) Close() error             { return nil }
func (o *overlayDB) Print() error             { return nil }
func (o *overlayDB) Stats() map[string]string { return nil }
func (o *overlayDB) NewBatch() dbm.Batch      { return &overlayBatch{db: o} }

type overlayOp struct {
	key, value []byte
	delete     bool
}

type overlayBatch struct {
	db  *overlayDB
	ops []overlayOp
}

func (b *overlayBatch) Set(key, value []byte) error {
	b.ops = append(b.ops, overlayOp{key: append([]byte{}, key...), value: append([]byte{}, value...)})
	return nil
}

func (b *overlayBatch) Delete(key []byte) error {
	b.ops = append(b.ops, overlayOp{key: append([]byte{}, key...), delete: true})
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
	b.ops = nil
	return nil
}

func (b *overlayBatch) WriteSync() error { return b.Write() }
func (b *overlayBatch) Close() error     { return nil }

// storePrefix is the prefix of a multistore substore's keys in the database.
func storePrefix(name string) []byte {
	return []byte("s/k:" + name + "/")
}

// hidePrefix shows a read-only parent without the keys under prefix. A store
// that an upgrade block adds must start empty, but the archive already holds
// its later versions, which IAVL would load instead.
func hidePrefix(parent dbm.DB, prefix []byte) dbm.DB {
	end := append(append([]byte{}, prefix[:len(prefix)-1]...), prefix[len(prefix)-1]+1)
	return &hiddenPrefixDB{DB: parent, prefix: prefix, end: end}
}

type hiddenPrefixDB struct {
	dbm.DB
	prefix, end []byte
}

func (h *hiddenPrefixDB) hidden(key []byte) bool { return bytes.HasPrefix(key, h.prefix) }

func (h *hiddenPrefixDB) Get(key []byte) ([]byte, error) {
	if h.hidden(key) {
		return nil, nil
	}
	return h.DB.Get(key)
}

func (h *hiddenPrefixDB) Has(key []byte) (bool, error) {
	if h.hidden(key) {
		return false, nil
	}
	return h.DB.Has(key)
}

func (h *hiddenPrefixDB) Iterator(start, end []byte) (dbm.Iterator, error) {
	return h.iterator(start, end, h.DB.Iterator)
}

func (h *hiddenPrefixDB) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	return h.iterator(start, end, h.DB.ReverseIterator)
}

// iterator returns an empty iterator for a range inside the prefix, so a
// substore scan never walks the hidden keys one by one.
func (h *hiddenPrefixDB) iterator(start, end []byte, open func(start, end []byte) (dbm.Iterator, error)) (dbm.Iterator, error) {
	if start != nil && end != nil && bytes.Compare(start, h.prefix) >= 0 && bytes.Compare(end, h.end) <= 0 {
		return dbm.NewMemDB().Iterator(start, end)
	}
	it, err := open(start, end)
	if err != nil {
		return nil, err
	}
	s := &skipIterator{Iterator: it, hidden: h.hidden}
	s.skip()
	return s, nil
}

type skipIterator struct {
	dbm.Iterator
	hidden func([]byte) bool
}

func (s *skipIterator) skip() {
	for s.Iterator.Valid() && s.hidden(s.Iterator.Key()) {
		s.Iterator.Next()
	}
}

func (s *skipIterator) Next() {
	s.Iterator.Next()
	s.skip()
}
