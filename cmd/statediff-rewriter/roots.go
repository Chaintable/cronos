package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ethereum/go-ethereum/common"
)

const rootRecordSize = 40
const rootSortChunkRecords = 1 << 20

type rootRecord struct {
	Root   common.Hash
	Height uint64
}

func writeRootRecord(writer io.Writer, root common.Hash, height uint64) error {
	var body [rootRecordSize]byte
	copy(body[:32], root[:])
	binary.BigEndian.PutUint64(body[32:], height)
	_, err := writer.Write(body[:])
	return err
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

func checkDuplicateRoots(rawPath, dir string) (string, string, error) {
	raw, err := os.Open(rawPath)
	if err != nil {
		return "", "", err
	}
	var chunks []string
	for chunkNumber := 0; ; chunkNumber++ {
		records := make([]rootRecord, 0, rootSortChunkRecords)
		for len(records) < cap(records) {
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
		chunkPath := filepath.Join(dir, fmt.Sprintf("roots-sort-%06d.tmp", chunkNumber))
		chunk, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = raw.Close()
			return "", "", err
		}
		for _, record := range records {
			if err := writeRootRecord(chunk, record.Root, record.Height); err != nil {
				_ = chunk.Close()
				_ = raw.Close()
				return "", "", err
			}
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
	if err := mergeRootChunks(chunks, sortedPath); err != nil {
		return "", "", err
	}
	for _, chunk := range chunks {
		if err := os.Remove(chunk); err != nil {
			return "", "", err
		}
	}
	if err := os.Remove(rawPath); err != nil {
		return "", "", err
	}
	hash, _, err := hashFile(sortedPath)
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

func mergeRootChunks(chunks []string, outputPath string) error {
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	readers := make([]*bufio.Reader, len(chunks))
	files := make([]*os.File, len(chunks))
	items := &rootHeap{}
	for i, path := range chunks {
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
		item := heap.Pop(items).(rootHeapItem)
		if havePrevious && item.record.Root == previous.Root && item.record.Height != previous.Height+1 {
			_ = output.Close()
			return fmt.Errorf("state root %s is reused by non-adjacent heights %d and %d", item.record.Root, previous.Height, item.record.Height)
		}
		if err := writeRootRecord(output, item.record.Root, item.record.Height); err != nil {
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
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return err
	}
	return output.Close()
}
