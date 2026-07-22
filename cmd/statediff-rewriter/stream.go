package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/rlp"
)

type packStream struct {
	dir             string
	manifest        planManifest
	chunkIndex      int
	pack            *os.File
	index           *os.File
	packReader      *bufio.Reader
	indexReader     *bufio.Reader
	chunkRecords    int64
	offset          int64
	expectedOrdinal uint64
	previousHeight  uint64
	peeked          *packRecord
	done            bool
}

func newPackStream(dir string, manifest planManifest) *packStream {
	return &packStream{dir: dir, manifest: manifest, expectedOrdinal: 1}
}

func (stream *packStream) openChunk() error {
	if stream.pack != nil || stream.chunkIndex >= len(stream.manifest.Chunks) {
		return nil
	}
	chunk := stream.manifest.Chunks[stream.chunkIndex]
	if chunk.Number != stream.chunkIndex || chunk.Records <= 0 {
		return fmt.Errorf("invalid pack chunk %d metadata", stream.chunkIndex)
	}
	pack, err := os.Open(filepath.Join(stream.dir, chunk.Pack))
	if err != nil {
		return err
	}
	index, err := os.Open(filepath.Join(stream.dir, chunk.Index))
	if err != nil {
		_ = pack.Close()
		return err
	}
	stream.pack, stream.index = pack, index
	stream.packReader, stream.indexReader = bufio.NewReader(pack), bufio.NewReader(index)
	stream.chunkRecords, stream.offset = 0, 0
	return nil
}

func (stream *packStream) closeChunk() error {
	if stream.pack == nil {
		return nil
	}
	chunk := stream.manifest.Chunks[stream.chunkIndex]
	if stream.chunkRecords != chunk.Records {
		return fmt.Errorf("pack chunk %s has %d records, want %d", chunk.Pack, stream.chunkRecords, chunk.Records)
	}
	if _, err := stream.packReader.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("pack chunk %s has trailing data", chunk.Pack)
		}
		return err
	}
	if _, err := stream.indexReader.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("pack index %s has trailing data", chunk.Index)
		}
		return err
	}
	packErr := stream.pack.Close()
	indexErr := stream.index.Close()
	stream.pack, stream.index = nil, nil
	stream.packReader, stream.indexReader = nil, nil
	stream.chunkIndex++
	return errors.Join(packErr, indexErr)
}

func (stream *packStream) read() (packRecord, bool, error) {
	for {
		if stream.done {
			return packRecord{}, false, nil
		}
		if err := stream.openChunk(); err != nil {
			return packRecord{}, false, err
		}
		if stream.pack == nil {
			stream.done = true
			if int64(stream.expectedOrdinal-1) != stream.manifest.Changed {
				return packRecord{}, false, fmt.Errorf("pack has %d records, manifest has %d", stream.expectedOrdinal-1, stream.manifest.Changed)
			}
			return packRecord{}, false, nil
		}
		chunk := stream.manifest.Chunks[stream.chunkIndex]
		if stream.chunkRecords == chunk.Records {
			if err := stream.closeChunk(); err != nil {
				return packRecord{}, false, err
			}
			continue
		}

		start := stream.offset
		var header [8]byte
		if _, err := io.ReadFull(stream.packReader, header[:]); err != nil {
			return packRecord{}, false, fmt.Errorf("read pack header %s: %w", chunk.Pack, err)
		}
		length := binary.BigEndian.Uint64(header[:])
		if length == 0 || length > uint64(maxObjectOperationBytes) {
			return packRecord{}, false, fmt.Errorf("invalid pack record length %d", length)
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(stream.packReader, body); err != nil {
			return packRecord{}, false, fmt.Errorf("read pack record %s: %w", chunk.Pack, err)
		}
		line, err := stream.indexReader.ReadBytes('\n')
		if err != nil {
			return packRecord{}, false, fmt.Errorf("read pack index %s: %w", chunk.Index, err)
		}
		var entry indexRecord
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&entry); err != nil {
			return packRecord{}, false, fmt.Errorf("decode pack index %s: %w", chunk.Index, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return packRecord{}, false, fmt.Errorf("pack index %s has trailing JSON", chunk.Index)
		}

		var record packRecord
		if err := rlp.DecodeBytes(body, &record); err != nil {
			return packRecord{}, false, fmt.Errorf("decode pack record %s: %w", chunk.Pack, err)
		}
		framedLength := length + 8
		if record.Schema != packSchema || record.Ordinal != stream.expectedOrdinal {
			return packRecord{}, false, fmt.Errorf("unexpected pack record schema/ordinal %d/%d", record.Schema, record.Ordinal)
		}
		if record.Height < uint64(stream.manifest.FirstHeight) || record.Height > uint64(stream.manifest.FinalHeight) ||
			(stream.previousHeight != 0 && record.Height <= stream.previousHeight) {
			return packRecord{}, false, fmt.Errorf("pack record height %d is outside or out of order for range %d-%d", record.Height, stream.manifest.FirstHeight, stream.manifest.FinalHeight)
		}
		if entry.Ordinal != record.Ordinal || entry.Height != record.Height || entry.Offset != start || entry.Length != framedLength ||
			entry.Key != record.Key || entry.OldSHA256 != hex.EncodeToString(record.OldSHA256[:]) ||
			entry.NewSHA256 != hex.EncodeToString(record.NewSHA256[:]) || entry.SlotsAdded != record.SlotsAdded ||
			entry.SlotsRemoved != record.SlotsRemoved || entry.SlotsChanged != record.SlotsChanged {
			return packRecord{}, false, fmt.Errorf("pack index %s disagrees with ordinal %d", chunk.Index, record.Ordinal)
		}
		if sha256Hash(record.OldBody) != record.OldSHA256 || sha256Hash(record.NewBody) != record.NewSHA256 {
			return packRecord{}, false, fmt.Errorf("pack record %d body hash mismatch", record.Ordinal)
		}

		stream.offset += int64(framedLength)
		stream.chunkRecords++
		stream.expectedOrdinal++
		stream.previousHeight = record.Height
		return record, true, nil
	}
}

func (stream *packStream) Peek() (packRecord, bool, error) {
	if stream.peeked != nil {
		return *stream.peeked, true, nil
	}
	record, ok, err := stream.read()
	if err != nil || !ok {
		return packRecord{}, ok, err
	}
	stream.peeked = &record
	return record, true, nil
}

func (stream *packStream) Next() (packRecord, bool, error) {
	if stream.peeked != nil {
		record := *stream.peeked
		stream.peeked = nil
		return record, true, nil
	}
	return stream.read()
}

func (stream *packStream) Close() error {
	if stream.pack == nil {
		return nil
	}
	packErr := stream.pack.Close()
	indexErr := stream.index.Close()
	stream.pack, stream.index = nil, nil
	return errors.Join(packErr, indexErr)
}

type heightRootStream struct {
	file     *os.File
	reader   *bufio.Reader
	expected uint64
}

func newHeightRootStream(path string, firstHeight uint64) (*heightRootStream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &heightRootStream{file: file, reader: bufio.NewReaderSize(file, 1<<20), expected: firstHeight}, nil
}

func (stream *heightRootStream) Next() (rootRecord, error) {
	record, err := readRootRecord(stream.reader)
	if err != nil {
		return rootRecord{}, err
	}
	if record.Height != stream.expected {
		return rootRecord{}, fmt.Errorf("height root index has height %d, want %d", record.Height, stream.expected)
	}
	stream.expected++
	return record, nil
}

func (stream *heightRootStream) Finish(finalHeight uint64) error {
	if stream.expected != finalHeight+1 {
		return fmt.Errorf("height root index ended before height %d", finalHeight)
	}
	if _, err := stream.reader.Peek(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("height root index has trailing records")
		}
		return err
	}
	return nil
}

func (stream *heightRootStream) Close() error { return stream.file.Close() }
