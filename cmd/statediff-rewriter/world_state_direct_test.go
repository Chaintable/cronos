package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

type worldStateFake struct {
	mu     sync.Mutex
	ranges map[string][][2]int64
	sets   map[string]map[int64]*iavl.ChangeSet
}

type worldStateFakeSource struct {
	fake  *worldStateFake
	label string
	start byte
	end   byte
}

func (source *worldStateFakeSource) VersionHash(version int64) ([]byte, error) {
	return bytes.Repeat([]byte{byte(version)}, legacyHashLength), nil
}

func (source *worldStateFakeSource) TraverseStateChanges(
	first, last int64,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	return source.TraverseStateChangesInKeyRange(first, last, []byte{source.start}, []byte{source.end}, callback)
}

func (source *worldStateFakeSource) TraverseStateChangesInKeyRange(
	first, last int64,
	start, end []byte,
	callback func(int64, *iavl.ChangeSet) error,
) error {
	if !bytes.Equal(start, []byte{source.start}) || !bytes.Equal(end, []byte{source.end}) {
		return fmt.Errorf("%s got key range %x-%x", source.label, start, end)
	}
	source.fake.mu.Lock()
	source.fake.ranges[source.label] = append(source.fake.ranges[source.label], [2]int64{first, last})
	source.fake.mu.Unlock()
	for version := first; version <= last; version++ {
		changeSet := &iavl.ChangeSet{}
		if source.fake.sets[source.label] != nil && source.fake.sets[source.label][version] != nil {
			changeSet = source.fake.sets[source.label][version]
		}
		if err := callback(version, changeSet); err != nil {
			return err
		}
	}
	return nil
}

func TestIterateParallelWorldStateDeltasReadsCodeAndStorageInOneEVMTraversal(t *testing.T) {
	code := []byte{0x60, 0x00}
	codeHash := crypto.Keccak256Hash(code)
	storageKey := make([]byte, 53)
	storageKey[0] = 0x02
	storageKey[20] = 0x01
	storageKey[52] = 0x02
	fake := &worldStateFake{
		ranges: make(map[string][][2]int64),
		sets: map[string]map[int64]*iavl.ChangeSet{
			"evm": {
				2: {Pairs: []*iavl.KVPair{
					{Key: append([]byte{evmCodePrefix}, codeHash.Bytes()...), Value: code},
					{Key: storageKey, Value: []byte{0x03}},
				}},
			},
		},
	}
	factory := func() worldStateSources {
		return worldStateSources{
			accounts: &worldStateFakeSource{fake: fake, label: "acc", start: 0x01, end: 0x02},
			balances: &worldStateFakeSource{fake: fake, label: "bank", start: 0x02, end: 0x03},
			evm:      &worldStateFakeSource{fake: fake, label: "evm", start: 0x01, end: 0x03},
		}
	}
	err := iterateParallelWorldStateDeltasContext(
		context.Background(), factory, "", []int64{2},
		1, directIAVLShardSize, 2, 2, false, true, true,
		func(delta worldStateDelta) error {
			require.Equal(t, []codeMutation{{CodeHash: codeHash, Code: code}}, delta.codes)
			require.Len(t, delta.storage, 1)
			require.Len(t, delta.storage[0].Values, 1)
			require.Equal(t, uint64(3), delta.storage[0].Values[0].Value.Uint64())
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, [][2]int64{{2, 2}}, fake.ranges["evm"])
	require.Empty(t, fake.ranges["acc"])
	require.Empty(t, fake.ranges["bank"])
}

func TestIterateParallelWorldStateDeltasReadsOnlySelectedStores(t *testing.T) {
	tests := []struct {
		name                     string
		accounts, codes, storage bool
		evmStart, evmEnd         byte
	}{
		{name: "accounts", accounts: true},
		{name: "codes", codes: true, evmStart: 0x01, evmEnd: 0x02},
		{name: "storage", storage: true, evmStart: 0x02, evmEnd: 0x03},
		{name: "code and storage", codes: true, storage: true, evmStart: 0x01, evmEnd: 0x03},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &worldStateFake{ranges: make(map[string][][2]int64)}
			factory := func() worldStateSources {
				sources := worldStateSources{}
				if test.accounts {
					sources.accounts = &worldStateFakeSource{fake: fake, label: "acc", start: 0x01, end: 0x02}
					sources.balances = &worldStateFakeSource{fake: fake, label: "bank", start: 0x02, end: 0x03}
				}
				if test.codes || test.storage {
					sources.evm = &worldStateFakeSource{
						fake: fake, label: "evm", start: test.evmStart, end: test.evmEnd,
					}
				}
				return sources
			}
			err := iterateParallelWorldStateDeltasContext(
				context.Background(), factory, "basecro", nil,
				1, directIAVLShardSize, 2, 2,
				test.accounts, test.codes, test.storage,
				func(worldStateDelta) error { return nil },
			)
			require.NoError(t, err)
			if test.accounts {
				require.Equal(t, [][2]int64{{2, 2}}, fake.ranges["acc"])
				require.Equal(t, [][2]int64{{2, 2}}, fake.ranges["bank"])
			} else {
				require.Empty(t, fake.ranges["acc"])
				require.Empty(t, fake.ranges["bank"])
			}
			if test.codes || test.storage {
				require.Equal(t, [][2]int64{{2, 2}}, fake.ranges["evm"])
			} else {
				require.Empty(t, fake.ranges["evm"])
			}
		})
	}
}

func TestIterateParallelWorldStateDeltasOrdersAndSplitsEveryLegacyBoundary(t *testing.T) {
	fake := &worldStateFake{ranges: make(map[string][][2]int64)}
	factory := func() worldStateSources {
		return worldStateSources{
			accounts: &worldStateFakeSource{fake: fake, label: "acc", start: 0x01, end: 0x02},
			balances: &worldStateFakeSource{fake: fake, label: "bank", start: 0x02, end: 0x03},
			evm:      &worldStateFakeSource{fake: fake, label: "evm", start: 0x01, end: 0x02},
		}
	}
	var heights []int64
	err := iterateParallelWorldStateDeltasContext(
		context.Background(), factory, "basecro",
		[]int64{70, 100, 120}, 3, directIAVLShardSize*2, 2, 200, true, true, false,
		func(delta worldStateDelta) error {
			heights = append(heights, delta.height)
			require.Empty(t, delta.accounts)
			require.Empty(t, delta.balances)
			require.Empty(t, delta.codes)
			require.Empty(t, delta.storage)
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, heights, 199)
	for index, height := range heights {
		require.Equal(t, int64(index+2), height)
	}
	for _, label := range []string{"acc", "bank", "evm"} {
		fake.mu.Lock()
		ranges := append([][2]int64(nil), fake.ranges[label]...)
		fake.mu.Unlock()
		require.ElementsMatch(t, [][2]int64{{2, 70}, {71, 100}, {101, 120}, {121, 200}}, ranges)
	}
}

func TestIterateParallelWorldStateDeltasRejectsInvalidArguments(t *testing.T) {
	err := iterateParallelWorldStateDeltasContext(
		context.Background(), func() worldStateSources { return worldStateSources{} }, "basecro",
		[]int64{-1}, 1, directIAVLShardSize, 2, 2,
		true, true, false, func(worldStateDelta) error { return nil },
	)
	require.ErrorContains(t, err, "invalid latest legacy version")
}
