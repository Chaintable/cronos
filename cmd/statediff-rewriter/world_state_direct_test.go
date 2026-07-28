package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"
)

type worldStateFake struct {
	mu     sync.Mutex
	ranges map[string][][2]int64
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
		if err := callback(version, &iavl.ChangeSet{}); err != nil {
			return err
		}
	}
	return nil
}

func TestIterateParallelWorldStateDeltasOrdersAndSplitsEveryLegacyBoundary(t *testing.T) {
	fake := &worldStateFake{ranges: make(map[string][][2]int64)}
	factory := func() worldStateSources {
		return worldStateSources{
			accounts: &worldStateFakeSource{fake: fake, label: "acc", start: 0x01, end: 0x02},
			balances: &worldStateFakeSource{fake: fake, label: "bank", start: 0x02, end: 0x03},
			codes:    &worldStateFakeSource{fake: fake, label: "evm", start: 0x01, end: 0x02},
		}
	}
	var heights []int64
	err := iterateParallelWorldStateDeltasContext(
		context.Background(), factory, "basecro",
		[]int64{70, 100, 120}, 3, directIAVLShardSize*2, 2, 200,
		func(delta worldStateDelta) error {
			heights = append(heights, delta.height)
			require.Empty(t, delta.accounts)
			require.Empty(t, delta.balances)
			require.Empty(t, delta.codes)
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
		[]int64{-1}, 1, directIAVLShardSize, 2, 2, func(worldStateDelta) error { return nil },
	)
	require.ErrorContains(t, err, "invalid latest legacy version")
}
