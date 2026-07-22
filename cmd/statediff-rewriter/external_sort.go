package main

import (
	"bufio"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/golang/snappy"
)

const (
	externalSortBufferSize     = 64 << 10
	externalRecordMemoryBytes  = int64(32)
	externalRecordLengthBytes  = 4
	externalSortDirectoryMatch = ".statediff-sort-*"
)

// externalSorter bounds its in-memory run by maxChunkBytes. The accounting
// includes each copied record and a fixed allowance for its slice and framing.
// Runs are written synchronously, so at most one run is being written at once.
type externalSorter struct {
	ctx            context.Context
	less           func([]byte, []byte) bool
	maxChunkBytes  int64
	maxChunks      int
	minFree        uint64
	allocationUnit uint64
	workDir        string
	chunkBytes     map[string]uint64
	ensureSpace    func(string, uint64) error
	records        [][]byte
	bufferedBytes  int64
	chunks         []string
	nextChunk      uint64
	finalized      bool
	closed         bool
	failed         error
	merge          *externalMergeReader
}

// newExternalSorter creates a sorter whose merge phase reads at most maxChunks
// source runs at once. Merge memory is bounded by one record and one fixed-size
// read buffer per source run.
func newExternalSorter(
	dir string,
	maxChunkBytes int64,
	maxChunks int,
	less func([]byte, []byte) bool,
) (*externalSorter, error) {
	return newExternalSorterWithContext(context.Background(), dir, maxChunkBytes, maxChunks, 0, less)
}

func newExternalSorterWithContext(
	ctx context.Context,
	dir string,
	maxChunkBytes int64,
	maxChunks int,
	minFree uint64,
	less func([]byte, []byte) bool,
) (*externalSorter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("external sorter context is required")
	}
	if dir == "" {
		return nil, fmt.Errorf("external sorter directory is required")
	}
	if maxChunkBytes < externalRecordMemoryBytes {
		return nil, fmt.Errorf("external sorter max chunk bytes must be at least %d", externalRecordMemoryBytes)
	}
	if maxChunks < 2 {
		return nil, fmt.Errorf("external sorter max chunks must be at least 2")
	}
	if less == nil {
		return nil, fmt.Errorf("external sorter comparison function is required")
	}
	workDir, err := os.MkdirTemp(dir, externalSortDirectoryMatch)
	if err != nil {
		return nil, fmt.Errorf("create external sorter directory: %w", err)
	}
	allocationUnit := uint64(1)
	if minFree != 0 {
		var filesystem syscall.Statfs_t
		if err := syscall.Statfs(workDir, &filesystem); err != nil {
			_ = os.RemoveAll(workDir)
			return nil, fmt.Errorf("inspect external sorter filesystem: %w", err)
		}
		if filesystem.Bsize <= 0 {
			_ = os.RemoveAll(workDir)
			return nil, fmt.Errorf("external sorter filesystem has invalid allocation unit %d", filesystem.Bsize)
		}
		allocationUnit = uint64(filesystem.Bsize)
	}
	return &externalSorter{
		ctx: ctx, less: less, maxChunkBytes: maxChunkBytes, maxChunks: maxChunks, minFree: minFree,
		allocationUnit: allocationUnit, workDir: workDir,
		chunkBytes: make(map[string]uint64), ensureSpace: ensureFreeSpace,
	}, nil
}

// Feed copies record. If adding it would exceed maxChunkBytes, Feed sorts and
// writes the current run before returning.
func (s *externalSorter) Feed(record []byte) error {
	if s == nil {
		return fmt.Errorf("external sorter is nil")
	}
	if s.closed {
		return fmt.Errorf("external sorter is closed")
	}
	if s.failed != nil {
		return s.failed
	}
	if err := s.ctx.Err(); err != nil {
		return s.fail(err)
	}
	if s.finalized {
		return fmt.Errorf("external sorter is finalized")
	}
	if uint64(len(record)) > math.MaxUint32 {
		return s.fail(fmt.Errorf("external sorter record length %d exceeds uint32", len(record)))
	}
	recordBytes := int64(len(record)) + externalRecordMemoryBytes
	if recordBytes > s.maxChunkBytes {
		return s.fail(fmt.Errorf("external sorter record requires %d bytes, exceeding chunk limit %d", recordBytes, s.maxChunkBytes))
	}
	if len(s.records) > 0 && s.bufferedBytes+recordBytes > s.maxChunkBytes {
		if err := s.flushChunk(); err != nil {
			return s.fail(err)
		}
	}
	copyOfRecord := append([]byte(nil), record...)
	s.records = append(s.records, copyOfRecord)
	s.bufferedBytes += recordBytes
	return nil
}

// Finalize writes the final run and returns a k-way merge reader. The reader
// owns the temporary runs after Finalize succeeds.
func (s *externalSorter) Finalize() (*externalMergeReader, error) {
	if s == nil {
		return nil, fmt.Errorf("external sorter is nil")
	}
	if s.closed {
		return nil, fmt.Errorf("external sorter is closed")
	}
	if s.failed != nil {
		return nil, s.failed
	}
	if s.finalized {
		return nil, fmt.Errorf("external sorter is already finalized")
	}
	if err := s.flushChunk(); err != nil {
		return nil, s.fail(err)
	}
	if err := s.mergePasses(); err != nil {
		return nil, s.fail(err)
	}
	merge, err := newExternalMergeReader(s)
	if err != nil {
		return nil, s.fail(err)
	}
	s.finalized = true
	s.merge = merge
	return merge, nil
}

// Close releases open readers and removes all temporary runs. It is idempotent.
func (s *externalSorter) Close() error {
	if s == nil || s.closed {
		return nil
	}
	if s.merge != nil {
		return s.merge.Close()
	}
	return s.cleanup()
}

func (s *externalSorter) fail(err error) error {
	if s.failed == nil {
		s.failed = err
	}
	return s.failed
}

func (s *externalSorter) flushChunk() error {
	if len(s.records) == 0 {
		return nil
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	sort.Slice(s.records, func(i, j int) bool {
		return s.less(s.records[i], s.records[j])
	})
	if err := s.ctx.Err(); err != nil {
		return err
	}
	var logicalBytes uint64
	for _, record := range s.records {
		recordBytes := uint64(externalRecordLengthBytes + len(record))
		if logicalBytes > math.MaxUint64-recordBytes {
			return fmt.Errorf("external sort chunk size overflows uint64")
		}
		logicalBytes += recordBytes
	}
	if err := s.ensureOutputSpace(logicalBytes); err != nil {
		return err
	}
	path := s.nextChunkPath()
	if err := writeExternalChunk(s.ctx, path, s.records); err != nil {
		return fmt.Errorf("write external sort chunk %d: %w", len(s.chunks), err)
	}
	s.chunks = append(s.chunks, path)
	s.chunkBytes[path] = logicalBytes
	s.records = nil
	s.bufferedBytes = 0
	return nil
}

func (s *externalSorter) ensureOutputSpace(logicalBytes uint64) error {
	if s.minFree == 0 {
		return nil
	}
	blocks := logicalBytes / (64 << 10)
	if logicalBytes%(64<<10) != 0 {
		blocks++
	}
	if blocks > (math.MaxUint64-10)/8 || logicalBytes > math.MaxUint64-blocks*8-10 {
		return fmt.Errorf("external sort output size overflows uint64")
	}
	encodedUpperBound := logicalBytes + blocks*8 + 10
	allocationBlocks := encodedUpperBound / s.allocationUnit
	if encodedUpperBound%s.allocationUnit != 0 {
		allocationBlocks++
	}
	maxBlocks := math.MaxUint64 / s.allocationUnit
	if maxBlocks < 2 || allocationBlocks > maxBlocks-2 {
		return fmt.Errorf("external sort allocated output size overflows uint64")
	}
	allocatedUpperBound := (allocationBlocks + 2) * s.allocationUnit
	if s.minFree > math.MaxUint64-allocatedUpperBound {
		return fmt.Errorf("external sort free-space requirement overflows uint64")
	}
	return s.ensureSpace(s.workDir, s.minFree+allocatedUpperBound)
}

func (s *externalSorter) nextChunkPath() string {
	path := filepath.Join(s.workDir, fmt.Sprintf("chunk-%012d.snappy", s.nextChunk))
	s.nextChunk++
	return path
}

// mergePasses reduces an arbitrary number of initial runs to maxChunks runs.
// It merges the smallest number of runs needed at each step and chooses the
// smallest runs first. Inputs are removed only after their replacement is
// durable, bounding peak disk use to current runs plus that selected group.
func (s *externalSorter) mergePasses() error {
	for len(s.chunks) > s.maxChunks {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		sort.Slice(s.chunks, func(i, j int) bool {
			left, right := s.chunkBytes[s.chunks[i]], s.chunkBytes[s.chunks[j]]
			if left != right {
				return left < right
			}
			return s.chunks[i] < s.chunks[j]
		})
		mergeCount := len(s.chunks) - s.maxChunks + 1
		if mergeCount > s.maxChunks {
			mergeCount = s.maxChunks
		}
		inputs := append([]string(nil), s.chunks[:mergeCount]...)
		var logicalBytes uint64
		for _, input := range inputs {
			inputBytes := s.chunkBytes[input]
			if logicalBytes > math.MaxUint64-inputBytes {
				return fmt.Errorf("merged external sort output size overflows uint64")
			}
			logicalBytes += inputBytes
		}
		if err := s.ensureOutputSpace(logicalBytes); err != nil {
			return err
		}
		output := s.nextChunkPath()
		if err := writeMergedExternalChunk(s.ctx, output, inputs, s.maxChunkBytes, s.less); err != nil {
			return fmt.Errorf("merge %d external sort chunks: %w", len(inputs), err)
		}
		for _, path := range inputs {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove merged external sort chunk %q: %w", path, err)
			}
			delete(s.chunkBytes, path)
		}
		s.chunkBytes[output] = logicalBytes
		s.chunks = append(append([]string(nil), s.chunks[mergeCount:]...), output)
	}
	return nil
}

func (s *externalSorter) cleanup() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.records = nil
	s.chunks = nil
	s.chunkBytes = nil
	if err := os.RemoveAll(s.workDir); err != nil {
		return fmt.Errorf("remove external sorter directory: %w", err)
	}
	return nil
}

func writeExternalChunk(ctx context.Context, path string, records [][]byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(path)
		}
	}()
	buffered := bufio.NewWriterSize(file, externalSortBufferSize)
	compressed := snappy.NewBufferedWriter(buffered)
	var writeErr error
	for _, record := range records {
		if writeErr = ctx.Err(); writeErr != nil {
			break
		}
		if writeErr = writeExternalRecord(compressed, record); writeErr != nil {
			break
		}
	}
	err = errors.Join(writeErr, compressed.Close(), buffered.Flush(), file.Sync(), file.Close())
	if err != nil {
		return err
	}
	ok = true
	return nil
}

func writeMergedExternalChunk(
	ctx context.Context,
	path string,
	inputs []string,
	maxChunkBytes int64,
	less func([]byte, []byte) bool,
) error {
	reader, err := newExternalMergeReaderForChunks(ctx, inputs, maxChunkBytes, less)
	if err != nil {
		return err
	}
	temporary := path + ".partial"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = reader.Close()
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(temporary)
			_ = os.Remove(path)
		}
	}()
	buffered := bufio.NewWriterSize(file, externalSortBufferSize)
	compressed := snappy.NewBufferedWriter(buffered)
	var writeErr error
	for {
		if writeErr = ctx.Err(); writeErr != nil {
			break
		}
		record, readErr := reader.Next()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			writeErr = readErr
			break
		}
		if writeErr = writeExternalRecord(compressed, record); writeErr != nil {
			break
		}
	}
	err = errors.Join(writeErr, compressed.Close(), buffered.Flush(), file.Sync(), file.Close(), reader.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	ok = true
	return nil
}

func writeExternalRecord(writer io.Writer, record []byte) error {
	var header [externalRecordLengthBytes]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(record)))
	if err := writeBytes(writer, header[:]); err != nil {
		return fmt.Errorf("write record length: %w", err)
	}
	if err := writeBytes(writer, record); err != nil {
		return fmt.Errorf("write record body: %w", err)
	}
	return nil
}

func readExternalRecord(reader io.Reader, maxChunkBytes int64) ([]byte, error) {
	var header [externalRecordLengthBytes]byte
	read, err := io.ReadFull(reader, header[:])
	if err != nil {
		if read == 0 && errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read record length: %w", err)
	}
	length := int64(binary.BigEndian.Uint32(header[:]))
	if length+externalRecordMemoryBytes > maxChunkBytes {
		return nil, fmt.Errorf("record length %d exceeds external sort chunk limit %d", length, maxChunkBytes)
	}
	record := make([]byte, int(length))
	if _, err := io.ReadFull(reader, record); err != nil {
		return nil, fmt.Errorf("read record body of length %d: %w", length, err)
	}
	return record, nil
}

type externalChunkReader struct {
	file   *os.File
	reader *snappy.Reader
}

type externalMergeItem struct {
	record []byte
	chunk  int
}

type externalMergeHeap struct {
	items []externalMergeItem
	less  func([]byte, []byte) bool
}

func (h externalMergeHeap) Len() int      { return len(h.items) }
func (h externalMergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h externalMergeHeap) Less(i, j int) bool {
	left, right := h.items[i], h.items[j]
	if h.less(left.record, right.record) {
		return true
	}
	if h.less(right.record, left.record) {
		return false
	}
	return left.chunk < right.chunk
}

func (h *externalMergeHeap) Push(value any) {
	h.items = append(h.items, value.(externalMergeItem))
}

func (h *externalMergeHeap) Pop() any {
	old := h.items
	last := len(old) - 1
	item := old[last]
	h.items = old[:last]
	return item
}

type externalMergeReader struct {
	ctx           context.Context
	owner         *externalSorter
	maxChunkBytes int64
	readers       []externalChunkReader
	heap          externalMergeHeap
	closed        bool
	failed        error
}

func newExternalMergeReader(owner *externalSorter) (*externalMergeReader, error) {
	merge, err := newExternalMergeReaderForChunks(owner.ctx, owner.chunks, owner.maxChunkBytes, owner.less)
	if err != nil {
		return nil, err
	}
	merge.owner = owner
	return merge, nil
}

func newExternalMergeReaderForChunks(
	ctx context.Context,
	chunks []string,
	maxChunkBytes int64,
	less func([]byte, []byte) bool,
) (*externalMergeReader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("external merge reader context is required")
	}
	merge := &externalMergeReader{
		ctx:           ctx,
		maxChunkBytes: maxChunkBytes,
		readers:       make([]externalChunkReader, len(chunks)),
		heap:          externalMergeHeap{less: less},
	}
	for index, path := range chunks {
		if err := ctx.Err(); err != nil {
			_ = merge.closeFiles()
			return nil, err
		}
		file, err := os.Open(path)
		if err != nil {
			_ = merge.closeFiles()
			return nil, fmt.Errorf("open external sort chunk %d: %w", index, err)
		}
		merge.readers[index] = externalChunkReader{
			file: file, reader: snappy.NewReader(bufio.NewReaderSize(file, externalSortBufferSize)),
		}
		record, err := readExternalRecord(merge.readers[index].reader, maxChunkBytes)
		if err != nil {
			_ = merge.closeFiles()
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("external sort chunk %d is empty", index)
			}
			return nil, fmt.Errorf("read external sort chunk %d: %w", index, err)
		}
		merge.heap.items = append(merge.heap.items, externalMergeItem{record: record, chunk: index})
	}
	heap.Init(&merge.heap)
	return merge, nil
}

// Next returns the next record or io.EOF after all records have been read.
func (r *externalMergeReader) Next() ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("external merge reader is nil")
	}
	if r.closed {
		return nil, fmt.Errorf("external merge reader is closed")
	}
	if r.failed != nil {
		return nil, r.failed
	}
	if err := r.ctx.Err(); err != nil {
		r.failed = err
		return nil, err
	}
	if r.heap.Len() == 0 {
		return nil, io.EOF
	}
	item := heap.Pop(&r.heap).(externalMergeItem)
	next, err := readExternalRecord(r.readers[item.chunk].reader, r.maxChunkBytes)
	switch {
	case err == nil:
		if r.heap.less(next, item.record) {
			r.failed = fmt.Errorf("external sort chunk %d is not sorted", item.chunk)
			return nil, r.failed
		}
		heap.Push(&r.heap, externalMergeItem{record: next, chunk: item.chunk})
	case errors.Is(err, io.EOF):
		// This run is exhausted.
	default:
		r.failed = fmt.Errorf("read external sort chunk %d: %w", item.chunk, err)
		return nil, r.failed
	}
	return item.record, nil
}

// Close releases all chunk files and removes the sorter's temporary directory.
func (r *externalMergeReader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	err := r.closeFiles()
	owner := r.owner
	r.owner = nil
	if owner != nil {
		owner.merge = nil
		err = errors.Join(err, owner.cleanup())
	}
	return err
}

func (r *externalMergeReader) closeFiles() error {
	var errs []error
	for index := range r.readers {
		if r.readers[index].file != nil {
			errs = append(errs, r.readers[index].file.Close())
			r.readers[index].file = nil
		}
	}
	return errors.Join(errs...)
}
