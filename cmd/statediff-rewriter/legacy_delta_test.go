package main

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

type sliceByteRecordReader struct {
	records [][]byte
	index   int
	err     error
}

func (reader *sliceByteRecordReader) Next() ([]byte, error) {
	if reader.index >= len(reader.records) {
		if reader.err != nil {
			return nil, reader.err
		}
		return nil, io.EOF
	}
	record := reader.records[reader.index]
	reader.index++
	return record, nil
}

func legacyStorageKey(marker byte) []byte {
	key := make([]byte, legacyStorageKeyLength)
	key[0] = 0x02
	key[len(key)-1] = marker
	return key
}

func mustLegacyDelta(t *testing.T, version int64, key []byte, kind legacyDeltaKind, value ...byte) []byte {
	t.Helper()
	body, err := encodeLegacyDelta(legacyDelta{Version: version, Key: key, Kind: kind, Value: value})
	require.NoError(t, err)
	return body
}

func TestReduceLegacyDeltas(t *testing.T) {
	keyA, keyB, keyC := legacyStorageKey(1), legacyStorageKey(2), legacyStorageKey(3)
	reader := &sliceByteRecordReader{records: [][]byte{
		mustLegacyDelta(t, 2, keyA, legacyDeltaNew, 1),
		mustLegacyDelta(t, 3, keyA, legacyDeltaOld),
		mustLegacyDelta(t, 3, keyA, legacyDeltaNew, 2),
		mustLegacyDelta(t, 3, keyB, legacyDeltaNew, 3),
		mustLegacyDelta(t, 5, keyB, legacyDeltaOld),
		mustLegacyDelta(t, 5, keyC, legacyDeltaNew, 4),
	}}

	changes := make(map[int64]*iavl.ChangeSet)
	require.NoError(t, reduceLegacyDeltas(context.Background(), reader, 1, 5, func(version int64, changeSet *iavl.ChangeSet) error {
		changes[version] = changeSet
		return nil
	}))
	require.Empty(t, changes[1].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: keyA, Value: []byte{1}}}, changes[2].Pairs)
	require.Equal(t, []*iavl.KVPair{{Key: keyA, Value: []byte{2}}, {Key: keyB, Value: []byte{3}}}, changes[3].Pairs)
	require.Empty(t, changes[4].Pairs)
	require.Equal(t, []*iavl.KVPair{{Delete: true, Key: keyB}, {Key: keyC, Value: []byte{4}}}, changes[5].Pairs)
}

func TestReduceLegacyDeltasRejectsDuplicatesAndReaderErrors(t *testing.T) {
	key := legacyStorageKey(1)
	t.Run("duplicate new", func(t *testing.T) {
		reader := &sliceByteRecordReader{records: [][]byte{
			mustLegacyDelta(t, 2, key, legacyDeltaNew, 1),
			mustLegacyDelta(t, 2, key, legacyDeltaNew, 2),
		}}
		err := reduceLegacyDeltas(context.Background(), reader, 1, 2, func(int64, *iavl.ChangeSet) error { return nil })
		require.ErrorContains(t, err, "duplicate new")
	})

	t.Run("reader", func(t *testing.T) {
		boom := errors.New("boom")
		err := reduceLegacyDeltas(context.Background(), &sliceByteRecordReader{err: boom}, 1, 2, func(int64, *iavl.ChangeSet) error { return nil })
		require.ErrorIs(t, err, boom)
	})

	t.Run("emit", func(t *testing.T) {
		boom := errors.New("boom")
		err := reduceLegacyDeltas(context.Background(), &sliceByteRecordReader{}, 1, 2, func(int64, *iavl.ChangeSet) error { return boom })
		require.ErrorIs(t, err, boom)
	})
}

func TestReduceLegacyDeltasHonorsCancellationAcrossEmptyVersions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	emitted := 0
	err := reduceLegacyDeltas(ctx, &sliceByteRecordReader{}, 1, 1_000_000, func(int64, *iavl.ChangeSet) error {
		emitted++
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, emitted)
}

func TestLegacyDeltaEncodingRejectsMalformedRecords(t *testing.T) {
	key := legacyStorageKey(1)
	_, err := encodeLegacyDelta(legacyDelta{Version: 1, Key: key, Kind: legacyDeltaOld, Value: []byte{1}})
	require.ErrorContains(t, err, "old delta contains a value")

	body := mustLegacyDelta(t, 1, key, legacyDeltaNew, 1)
	body[len(body)-2] = 2
	_, err = decodeLegacyDelta(body)
	require.ErrorContains(t, err, "value length")
}
