package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ethereum/go-ethereum/common"
)

const (
	rootRecordSize       = 40
	rootSortChunkRecords = 1 << 20
)

type rootRecord struct {
	Root   common.Hash
	Height uint64
}

func writeRootRecord(writer io.Writer, root common.Hash, height uint64) error {
	body := encodeRootRecord(rootRecord{Root: root, Height: height})
	return writeBytes(writer, body[:])
}

func encodeRootRecord(record rootRecord) [rootRecordSize]byte {
	var body [rootRecordSize]byte
	copy(body[:32], record.Root[:])
	binary.BigEndian.PutUint64(body[32:], record.Height)
	return body
}

func readRootRecord(reader io.Reader) (rootRecord, error) {
	var body [rootRecordSize]byte
	if _, err := io.ReadFull(reader, body[:]); err != nil {
		return rootRecord{}, err
	}
	return rootRecord{Root: common.BytesToHash(body[:32]), Height: binary.BigEndian.Uint64(body[32:])}, nil
}

func rootLess(left, right rootRecord) bool {
	if comparison := bytes.Compare(left.Root[:], right.Root[:]); comparison != 0 {
		return comparison < 0
	}
	return left.Height < right.Height
}

// rootMultisetSHA256 commits to the count and order-independent XOR and sum of
// the SHA-256 digest of every root record. Duplicate records affect the sum and
// count, so the result commits to a multiset rather than a set.
func rootMultisetSHA256(path string) (string, int64, error) {
	return rootMultisetSHA256Context(context.Background(), path)
}

func rootMultisetSHA256Context(ctx context.Context, path string) (string, int64, error) {
	if ctx == nil {
		return "", 0, fmt.Errorf("root multiset context is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1<<20)
	var xor, sum [sha256.Size]byte
	var count uint64
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		record, err := readRootRecord(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", 0, fmt.Errorf("read root multiset: %w", err)
		}
		body := encodeRootRecord(record)
		digest := sha256.Sum256(body[:])
		carry := uint16(0)
		for i := len(digest) - 1; i >= 0; i-- {
			xor[i] ^= digest[i]
			total := uint16(sum[i]) + uint16(digest[i]) + carry
			sum[i] = byte(total)
			carry = total >> 8
		}
		count++
	}
	var commitment [8 + 2*sha256.Size]byte
	binary.BigEndian.PutUint64(commitment[:8], count)
	copy(commitment[8:8+sha256.Size], xor[:])
	copy(commitment[8+sha256.Size:], sum[:])
	result := sha256.Sum256(commitment[:])
	return hex.EncodeToString(result[:]), int64(count), nil
}

func checkDuplicateRoots(rawPath, dir string) (string, string, error) {
	return checkDuplicateRootsContext(context.Background(), rawPath, dir)
}

func checkDuplicateRootsContext(ctx context.Context, rawPath, dir string) (string, string, error) {
	if ctx == nil {
		return "", "", fmt.Errorf("root sort context is required")
	}
	raw, err := os.Open(rawPath)
	if err != nil {
		return "", "", err
	}
	var chunks []string
	for chunkNumber := 0; ; chunkNumber++ {
		records := make([]rootRecord, 0, rootSortChunkRecords)
		for len(records) < cap(records) {
			if err := ctx.Err(); err != nil {
				_ = raw.Close()
				return "", "", err
			}
			record, err := readRootRecord(raw)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = raw.Close()
				return "", "", fmt.Errorf("read root index: %w", err)
			}
			records = append(records, record)
		}
		if len(records) == 0 {
			break
		}
		sort.Slice(records, func(i, j int) bool { return rootLess(records[i], records[j]) })
		if err := ctx.Err(); err != nil {
			_ = raw.Close()
			return "", "", err
		}
		chunkPath := filepath.Join(dir, fmt.Sprintf("roots-sort-%06d.tmp", chunkNumber))
		chunk, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = raw.Close()
			return "", "", err
		}
		chunkWriter := bufio.NewWriterSize(chunk, 1<<20)
		for _, record := range records {
			if err := ctx.Err(); err != nil {
				_ = chunk.Close()
				_ = raw.Close()
				return "", "", err
			}
			if err := writeRootRecord(chunkWriter, record.Root, record.Height); err != nil {
				_ = chunk.Close()
				_ = raw.Close()
				return "", "", err
			}
		}
		if err := chunkWriter.Flush(); err != nil {
			_ = chunk.Close()
			_ = raw.Close()
			return "", "", err
		}
		if err := chunk.Sync(); err != nil {
			_ = chunk.Close()
			_ = raw.Close()
			return "", "", err
		}
		if err := chunk.Close(); err != nil {
			_ = raw.Close()
			return "", "", err
		}
		chunks = append(chunks, chunkPath)
		if len(records) < rootSortChunkRecords {
			break
		}
	}
	if err := raw.Close(); err != nil {
		return "", "", err
	}
	sortedName := "roots.sorted"
	sortedPath := filepath.Join(dir, sortedName)
	if err := mergeRootChunksContext(ctx, chunks, sortedPath); err != nil {
		return "", "", err
	}
	for _, chunk := range chunks {
		if err := os.Remove(chunk); err != nil {
			return "", "", err
		}
	}
	hash, _, err := hashFileContext(ctx, sortedPath)
	return sortedName, hash, err
}

type rootHeapItem struct {
	record rootRecord
	reader int
}

type rootHeap []rootHeapItem

func (h rootHeap) Len() int           { return len(h) }
func (h rootHeap) Less(i, j int) bool { return rootLess(h[i].record, h[j].record) }
func (h rootHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rootHeap) Push(value any)    { *h = append(*h, value.(rootHeapItem)) }
func (h *rootHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

func mergeRootChunksContext(ctx context.Context, chunks []string, outputPath string) error {
	if ctx == nil {
		return fmt.Errorf("root merge context is required")
	}
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	outputWriter := bufio.NewWriterSize(output, 1<<20)
	readers := make([]*bufio.Reader, len(chunks))
	files := make([]*os.File, len(chunks))
	items := &rootHeap{}
	for i, path := range chunks {
		if err := ctx.Err(); err != nil {
			_ = output.Close()
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			_ = output.Close()
			return err
		}
		files[i] = file
		readers[i] = bufio.NewReader(file)
		record, err := readRootRecord(readers[i])
		if err == nil {
			heap.Push(items, rootHeapItem{record: record, reader: i})
		} else if !errors.Is(err, io.EOF) {
			_ = output.Close()
			return err
		}
	}
	heap.Init(items)
	var previous rootRecord
	havePrevious := false
	for items.Len() > 0 {
		if err := ctx.Err(); err != nil {
			_ = output.Close()
			return err
		}
		item := heap.Pop(items).(rootHeapItem)
		if havePrevious && item.record.Root == previous.Root && item.record.Height != previous.Height+1 {
			_ = output.Close()
			return fmt.Errorf("state root %s is reused by non-adjacent heights %d and %d", item.record.Root, previous.Height, item.record.Height)
		}
		if err := writeRootRecord(outputWriter, item.record.Root, item.record.Height); err != nil {
			_ = output.Close()
			return err
		}
		previous, havePrevious = item.record, true
		next, err := readRootRecord(readers[item.reader])
		if err == nil {
			heap.Push(items, rootHeapItem{record: next, reader: item.reader})
		} else if !errors.Is(err, io.EOF) {
			_ = output.Close()
			return err
		}
	}
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
	if err := outputWriter.Flush(); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return err
	}
	return output.Close()
}
