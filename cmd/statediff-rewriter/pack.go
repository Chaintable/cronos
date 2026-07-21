package main

import (
	"bufio"
	"crypto/sha256"
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

type indexRecord struct {
	Ordinal   uint64 `json:"ordinal"`
	Height    uint64 `json:"height"`
	Offset    int64  `json:"offset"`
	Length    uint64 `json:"length"`
	Key       string `json:"key"`
	OldSHA256 string `json:"old_sha256"`
	NewSHA256 string `json:"new_sha256"`
}

type packWriter struct {
	dir       string
	maxSize   int64
	chunk     int
	pack      *os.File
	index     *os.File
	offset    int64
	records   int64
	chunks    []chunkManifest
	finalized bool
}

func newPackWriter(dir string, maxSize int64) (*packWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &packWriter{dir: dir, maxSize: maxSize}, nil
}

func (w *packWriter) openChunk() error {
	if w.pack != nil {
		return nil
	}
	pack, err := os.OpenFile(filepath.Join(w.dir, fmt.Sprintf("chunk-%06d.pack.tmp", w.chunk)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	index, err := os.OpenFile(filepath.Join(w.dir, fmt.Sprintf("chunk-%06d.idx.tmp", w.chunk)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		_ = pack.Close()
		return err
	}
	w.pack, w.index = pack, index
	w.offset, w.records = 0, 0
	return nil
}

func (w *packWriter) Write(record packRecord) error {
	if w.finalized {
		return fmt.Errorf("pack writer is finalized")
	}
	body, err := rlp.EncodeToBytes(record)
	if err != nil {
		return err
	}
	framedSize := int64(8 + len(body))
	if w.pack != nil && w.records > 0 && w.offset+framedSize > w.maxSize {
		if err := w.closeChunk(); err != nil {
			return err
		}
	}
	if err := w.openChunk(); err != nil {
		return err
	}
	start := w.offset
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], uint64(len(body)))
	if _, err := w.pack.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.pack.Write(body); err != nil {
		return err
	}
	entry := indexRecord{
		Ordinal: record.Ordinal, Height: record.Height, Offset: start, Length: uint64(framedSize), Key: record.Key,
		OldSHA256: hex.EncodeToString(record.OldSHA256[:]), NewSHA256: hex.EncodeToString(record.NewSHA256[:]),
	}
	indexBody, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := w.index.Write(append(indexBody, '\n')); err != nil {
		return err
	}
	w.offset += framedSize
	w.records++
	return nil
}

func (w *packWriter) closeChunk() error {
	if w.pack == nil {
		return nil
	}
	if err := w.pack.Sync(); err != nil {
		return err
	}
	if err := w.index.Sync(); err != nil {
		return err
	}
	if err := w.pack.Close(); err != nil {
		return err
	}
	if err := w.index.Close(); err != nil {
		return err
	}
	packTmp := filepath.Join(w.dir, fmt.Sprintf("chunk-%06d.pack.tmp", w.chunk))
	indexTmp := filepath.Join(w.dir, fmt.Sprintf("chunk-%06d.idx.tmp", w.chunk))
	packName := fmt.Sprintf("chunk-%06d.pack", w.chunk)
	indexName := fmt.Sprintf("chunk-%06d.idx", w.chunk)
	packPath := filepath.Join(w.dir, packName)
	indexPath := filepath.Join(w.dir, indexName)
	if err := os.Rename(packTmp, packPath); err != nil {
		return err
	}
	if err := os.Rename(indexTmp, indexPath); err != nil {
		return err
	}
	packHash, packSize, err := hashFile(packPath)
	if err != nil {
		return err
	}
	indexHash, _, err := hashFile(indexPath)
	if err != nil {
		return err
	}
	w.chunks = append(w.chunks, chunkManifest{
		Number: w.chunk, Pack: packName, PackSHA256: packHash, PackSize: packSize,
		Index: indexName, IndexSHA256: indexHash, Records: w.records,
	})
	w.chunk++
	w.pack, w.index = nil, nil
	w.offset, w.records = 0, 0
	return syncDir(w.dir)
}

func (w *packWriter) Close() ([]chunkManifest, error) {
	if w.finalized {
		return nil, fmt.Errorf("pack writer already finalized")
	}
	w.finalized = true
	if err := w.closeChunk(); err != nil {
		return nil, err
	}
	return append([]chunkManifest(nil), w.chunks...), nil
}

func hashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hasher := sha256.New()
	n, err := io.Copy(hasher, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), n, nil
}

func iteratePack(dir string, manifest planManifest, fn func(packRecord) error) error {
	expectedOrdinal := uint64(1)
	for _, chunk := range manifest.Chunks {
		packPath := filepath.Join(dir, chunk.Pack)
		packHash, packSize, err := hashFile(packPath)
		if err != nil {
			return err
		}
		if packHash != chunk.PackSHA256 || packSize != chunk.PackSize {
			return fmt.Errorf("pack chunk changed: %s", chunk.Pack)
		}
		indexHash, _, err := hashFile(filepath.Join(dir, chunk.Index))
		if err != nil {
			return err
		}
		if indexHash != chunk.IndexSHA256 {
			return fmt.Errorf("pack index changed: %s", chunk.Index)
		}
		file, err := os.Open(packPath)
		if err != nil {
			return err
		}
		reader := bufio.NewReader(file)
		var records int64
		for {
			var header [8]byte
			_, err := io.ReadFull(reader, header[:])
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = file.Close()
				return fmt.Errorf("read pack header %s: %w", chunk.Pack, err)
			}
			length := binary.BigEndian.Uint64(header[:])
			if length == 0 || length > uint64(maxObjectSize*2+(1<<20)) {
				_ = file.Close()
				return fmt.Errorf("invalid pack record length %d", length)
			}
			body := make([]byte, length)
			if _, err := io.ReadFull(reader, body); err != nil {
				_ = file.Close()
				return fmt.Errorf("read pack record %s: %w", chunk.Pack, err)
			}
			var record packRecord
			if err := rlp.DecodeBytes(body, &record); err != nil {
				_ = file.Close()
				return fmt.Errorf("decode pack record %s: %w", chunk.Pack, err)
			}
			if record.Schema != packSchema || record.Ordinal != expectedOrdinal {
				_ = file.Close()
				return fmt.Errorf("unexpected pack record schema/ordinal %d/%d", record.Schema, record.Ordinal)
			}
			if err := fn(record); err != nil {
				_ = file.Close()
				return err
			}
			expectedOrdinal++
			records++
		}
		if err := file.Close(); err != nil {
			return err
		}
		if records != chunk.Records {
			return fmt.Errorf("pack chunk %s has %d records, want %d", chunk.Pack, records, chunk.Records)
		}
	}
	if int64(expectedOrdinal-1) != manifest.Changed {
		return fmt.Errorf("pack has %d records, manifest has %d", expectedOrdinal-1, manifest.Changed)
	}
	return nil
}
