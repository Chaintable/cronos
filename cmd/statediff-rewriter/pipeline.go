package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	defaultObjectConcurrency      = 96
	defaultObjectWindow           = 384
	defaultMaxInFlightBytes       = int64(8 << 30)
	defaultCheckpointEvery        = uint64(4096)
	defaultCheckpointInterval     = time.Second
	maximumObjectConcurrency      = 4096
	maximumOrderedPipelineWindow  = 1 << 20
	minimumOrderedPipelineWorkers = 1
	maxObjectOperationBytes       = maxObjectSize*2 + (1 << 20)
)

type parallelOptions struct {
	Concurrency        int
	Window             int
	MaxInFlightBytes   int64
	CheckpointEvery    uint64
	CheckpointInterval time.Duration
}

func defaultParallelOptions() parallelOptions {
	return parallelOptions{
		Concurrency: defaultObjectConcurrency, Window: defaultObjectWindow,
		MaxInFlightBytes: defaultMaxInFlightBytes,
		CheckpointEvery:  defaultCheckpointEvery, CheckpointInterval: defaultCheckpointInterval,
	}
}

func (options parallelOptions) validate(withCheckpoint bool) error {
	if options.Concurrency < minimumOrderedPipelineWorkers || options.Concurrency > maximumObjectConcurrency {
		return fmt.Errorf("concurrency must be between %d and %d", minimumOrderedPipelineWorkers, maximumObjectConcurrency)
	}
	if options.Window < options.Concurrency || options.Window > maximumOrderedPipelineWindow {
		return fmt.Errorf("window must be between concurrency (%d) and %d", options.Concurrency, maximumOrderedPipelineWindow)
	}
	if options.MaxInFlightBytes < maxObjectOperationBytes {
		return fmt.Errorf("max-inflight-bytes must be at least %d", maxObjectOperationBytes)
	}
	if withCheckpoint && (options.CheckpointEvery == 0 || options.CheckpointInterval <= 0) {
		return fmt.Errorf("checkpoint interval and record count must be positive")
	}
	return nil
}

type byteLimiter struct {
	limit int64
	bytes *semaphore.Weighted
}

func newByteLimiter(limit int64) (*byteLimiter, error) {
	if limit < maxObjectOperationBytes {
		return nil, fmt.Errorf("byte limit must be at least %d", maxObjectOperationBytes)
	}
	return &byteLimiter{limit: limit, bytes: semaphore.NewWeighted(limit)}, nil
}

func (limiter *byteLimiter) Acquire(ctx context.Context, amount int64) error {
	if amount <= 0 || amount > limiter.limit {
		return fmt.Errorf("in-flight byte reservation %d is outside 1-%d", amount, limiter.limit)
	}
	return limiter.bytes.Acquire(ctx, amount)
}

func (limiter *byteLimiter) Release(amount int64) {
	limiter.bytes.Release(amount)
}

func estimatedPackRecordBytes(record packRecord) int64 {
	// The RLP frame and decoded record can coexist while iteratePack invokes its
	// callback. Count the two object bodies twice and leave fixed room for the
	// record, hashes, decoded metadata, and allocator overhead.
	return int64(2*(len(record.OldBody)+len(record.NewBody))+
		len(record.Key)+len(record.OldETag)+len(record.Headers.Metadata)+
		len(record.Headers.CacheControl)+len(record.Headers.ContentDisposition)+
		len(record.Headers.ContentEncoding)+len(record.Headers.ContentLanguage)+
		len(record.Headers.ContentType)+len(record.Headers.WebsiteRedirect)) + (1 << 20)
}

type orderedTask[T any] struct {
	sequence uint64
	value    T
}

type orderedResult[T any] struct {
	sequence uint64
	value    T
	err      error
}

type producerSummary struct {
	count uint64
	err   error
}

// runOrderedPipeline executes work concurrently while committing successful results in
// contiguous sequence order. The producer must emit every sequence exactly once starting
// at firstSequence. The window bounds emitted-but-not-committed work, including reordering.
func runOrderedPipeline[Input, Output any](
	ctx context.Context,
	firstSequence uint64,
	workers int,
	window int,
	produce func(context.Context, func(uint64, Input) error) error,
	work func(context.Context, Input) (Output, error),
	commit func(uint64, Output) error,
) error {
	if firstSequence == 0 {
		return fmt.Errorf("first sequence must be positive")
	}
	if workers < 1 || workers > maximumObjectConcurrency {
		return fmt.Errorf("workers must be between 1 and %d", maximumObjectConcurrency)
	}
	if window < workers || window > maximumOrderedPipelineWindow {
		return fmt.Errorf("window must be between workers (%d) and %d", workers, maximumOrderedPipelineWindow)
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan orderedTask[Input], workers)
	results := make(chan orderedResult[Output], workers)
	tokens := make(chan struct{}, window)
	producerDone := make(chan producerSummary, 1)

	go func() {
		expected := firstSequence
		var count uint64
		emit := func(sequence uint64, value Input) error {
			if sequence != expected {
				return fmt.Errorf("producer emitted sequence %d, want %d", sequence, expected)
			}
			select {
			case tokens <- struct{}{}:
			case <-pipelineCtx.Done():
				return pipelineCtx.Err()
			}
			select {
			case jobs <- orderedTask[Input]{sequence: sequence, value: value}:
				expected++
				count++
				return nil
			case <-pipelineCtx.Done():
				<-tokens
				return pipelineCtx.Err()
			}
		}
		err := produce(pipelineCtx, emit)
		producerDone <- producerSummary{count: count, err: err}
		close(jobs)
		if err != nil {
			cancel()
		}
	}()

	var workerGroup sync.WaitGroup
	workerGroup.Add(workers)
	for range workers {
		go func() {
			defer workerGroup.Done()
			for task := range jobs {
				value, err := work(pipelineCtx, task.value)
				result := orderedResult[Output]{sequence: task.sequence, value: value, err: err}
				if err != nil {
					// Deliver the causal error before cancellation makes peers exit.
					results <- result
					cancel()
					return
				}
				select {
				case results <- result:
				case <-pipelineCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		workerGroup.Wait()
		close(results)
	}()

	next := firstSequence
	pending := make(map[uint64]Output, workers)
	var causalErr error
	for result := range results {
		if result.err != nil {
			if causalErr == nil || errors.Is(causalErr, context.Canceled) {
				causalErr = fmt.Errorf("sequence %d: %w", result.sequence, result.err)
			}
			continue
		}
		if causalErr != nil {
			continue
		}
		if result.sequence < next {
			causalErr = fmt.Errorf("worker returned committed sequence %d before %d", result.sequence, next)
			cancel()
			continue
		}
		if _, exists := pending[result.sequence]; exists {
			causalErr = fmt.Errorf("worker returned duplicate sequence %d", result.sequence)
			cancel()
			continue
		}
		pending[result.sequence] = result.value
		for {
			value, exists := pending[next]
			if !exists {
				break
			}
			if err := commit(next, value); err != nil {
				causalErr = fmt.Errorf("commit sequence %d: %w", next, err)
				cancel()
				break
			}
			delete(pending, next)
			<-tokens
			next++
		}
	}

	summary := <-producerDone
	if summary.err != nil && !errors.Is(summary.err, context.Canceled) &&
		(causalErr == nil || errors.Is(causalErr, context.Canceled)) {
		causalErr = summary.err
	}
	if causalErr != nil {
		return causalErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if next != firstSequence+summary.count {
		return fmt.Errorf("pipeline committed through %d after producing %d records from %d", next-1, summary.count, firstSequence)
	}
	if len(pending) != 0 || len(tokens) != 0 {
		return fmt.Errorf("pipeline finished with %d pending results and %d window tokens", len(pending), len(tokens))
	}
	return nil
}

type checkpointBatcher struct {
	path        string
	checkpoint  checkpoint
	every       uint64
	interval    time.Duration
	advances    uint64
	dirty       bool
	lastPersist time.Time
	now         func() time.Time
	save        func(string, checkpoint) error
}

func newCheckpointBatcher(path string, cp checkpoint, every uint64, interval time.Duration) (*checkpointBatcher, error) {
	if every == 0 || interval <= 0 {
		return nil, fmt.Errorf("checkpoint interval and record count must be positive")
	}
	return &checkpointBatcher{
		path: path, checkpoint: cp, every: every, interval: interval,
		lastPersist: time.Now(), now: time.Now, save: saveCheckpoint,
	}, nil
}

func (batcher *checkpointBatcher) Advance(frontier, height uint64) error {
	if frontier <= batcher.checkpoint.Frontier {
		return fmt.Errorf("checkpoint frontier %d does not advance beyond %d", frontier, batcher.checkpoint.Frontier)
	}
	batcher.checkpoint.Frontier, batcher.checkpoint.Height = frontier, height
	batcher.advances++
	batcher.dirty = true
	if batcher.advances >= batcher.every || batcher.now().Sub(batcher.lastPersist) >= batcher.interval {
		return batcher.Flush()
	}
	return nil
}

func (batcher *checkpointBatcher) Flush() error {
	if !batcher.dirty {
		return nil
	}
	if err := batcher.save(batcher.path, batcher.checkpoint); err != nil {
		return err
	}
	batcher.advances = 0
	batcher.dirty = false
	batcher.lastPersist = batcher.now()
	return nil
}
