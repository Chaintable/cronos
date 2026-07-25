package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrderedPipelineCommitsOutOfOrderWorkInSequence(t *testing.T) {
	var committed []int
	err := runOrderedPipeline(context.Background(), 1, 4, 8,
		func(_ context.Context, emit func(uint64, int) error) error {
			for sequence := uint64(1); sequence <= 20; sequence++ {
				if err := emit(sequence, int(sequence)); err != nil {
					return err
				}
			}
			return nil
		},
		func(_ context.Context, value int) (int, error) {
			time.Sleep(time.Duration(20-value%5) * time.Millisecond)
			return value * 2, nil
		},
		func(sequence uint64, value int) error {
			require.Equal(t, int(sequence)*2, value)
			committed = append(committed, int(sequence))
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, committed)
}

func TestOrderedPipelineStopsFrontierAtFailureGap(t *testing.T) {
	boom := errors.New("boom")
	var committed []uint64
	err := runOrderedPipeline(context.Background(), 1, 4, 8,
		func(_ context.Context, emit func(uint64, int) error) error {
			for sequence := uint64(1); sequence <= 20; sequence++ {
				if err := emit(sequence, int(sequence)); err != nil {
					return err
				}
			}
			return nil
		},
		func(_ context.Context, value int) (int, error) {
			if value == 5 {
				return 0, boom
			}
			return value, nil
		},
		func(sequence uint64, _ int) error {
			committed = append(committed, sequence)
			return nil
		},
	)
	require.ErrorIs(t, err, boom)
	for i, sequence := range committed {
		require.Equal(t, uint64(i+1), sequence)
		require.Less(t, sequence, uint64(5))
	}
}

func TestOrderedPipelineBoundsUncommittedWindow(t *testing.T) {
	const window = 7
	var active, maximum atomic.Int64
	gate := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	done := make(chan error, 1)
	go func() {
		done <- runOrderedPipeline(context.Background(), 1, 4, window,
			func(_ context.Context, emit func(uint64, int) error) error {
				for sequence := uint64(1); sequence <= 50; sequence++ {
					if err := emit(sequence, int(sequence)); err != nil {
						return err
					}
				}
				return nil
			},
			func(_ context.Context, value int) (int, error) {
				current := active.Add(1)
				for {
					previous := maximum.Load()
					if current <= previous || maximum.CompareAndSwap(previous, current) {
						break
					}
				}
				once.Do(func() { close(started) })
				<-gate
				active.Add(-1)
				return value, nil
			},
			func(uint64, int) error { return nil },
		)
	}()
	<-started
	time.Sleep(25 * time.Millisecond)
	require.LessOrEqual(t, maximum.Load(), int64(4))
	close(gate)
	require.NoError(t, <-done)
}

func TestOrderedPipelineRejectsProducerGap(t *testing.T) {
	err := runOrderedPipeline(context.Background(), 10, 1, 1,
		func(_ context.Context, emit func(uint64, int) error) error {
			return emit(11, 1)
		},
		func(_ context.Context, value int) (int, error) { return value, nil },
		func(uint64, int) error { return nil },
	)
	require.ErrorContains(t, err, "emitted sequence 11, want 10")
}

func TestCheckpointBatcherPersistsByCountTimeAndFinalFlush(t *testing.T) {
	now := time.Unix(100, 0)
	var saved []checkpoint
	batcher, err := newCheckpointBatcher("checkpoint", checkpoint{RunID: "run"}, 3, time.Second)
	require.NoError(t, err)
	batcher.now = func() time.Time { return now }
	batcher.lastPersist = now
	batcher.save = func(_ string, cp checkpoint) error {
		saved = append(saved, cp)
		return nil
	}

	require.NoError(t, batcher.Advance(1, 10))
	require.NoError(t, batcher.Advance(2, 11))
	require.Empty(t, saved)
	require.NoError(t, batcher.Advance(3, 12))
	require.Equal(t, []checkpoint{{RunID: "run", Frontier: 3, Height: 12}}, saved)

	now = now.Add(time.Second)
	require.NoError(t, batcher.Advance(4, 13))
	require.Len(t, saved, 2)
	require.Equal(t, uint64(4), saved[1].Frontier)

	require.NoError(t, batcher.Advance(5, 14))
	require.NoError(t, batcher.Flush())
	require.Len(t, saved, 3)
	require.Equal(t, uint64(5), saved[2].Frontier)
	require.NoError(t, batcher.Flush())
	require.Len(t, saved, 3)
}
