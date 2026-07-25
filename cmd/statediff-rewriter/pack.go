package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/ethereum/go-ethereum/rlp"
)

type indexRecord struct {
	Ordinal      uint64 `json:"ordinal"`
	Height       uint64 `json:"height"`
	Offset       int64  `json:"offset"`
	Length       uint64 `json:"length"`
	Key          string `json:"key"`
	OldSHA256    string `json:"old_sha256"`
	NewSHA256    string `json:"new_sha256"`
	SlotsAdded   uint64 `json:"slots_added"`
	SlotsRemoved uint64 `json:"slots_removed"`
	SlotsChanged uint64 `json:"slots_changed"`
}

type packWriter struct {
	ctx         context.Context
	dir         string
	maxSize     int64
	chunk       int
	pack        *os.File
	index       *os.File
	packBuffer  *bufio.Writer
	indexBuffer *bufio.Writer
	offset      int64
	indexOffset int64
	records     int64
	chunks      []chunkManifest
	finalized   bool
}

type packWriterState struct {
	Chunk       int             `json:"chunk"`
	PackOffset  int64           `json:"pack_offset"`
	IndexOffset int64           `json:"index_offset"`
	Records     int64           `json:"records"`
	Chunks      []chunkManifest `json:"chunks"`
}

func newPackWriter(dir string, maxSize int64) (*packWriter, error) {
	return newPackWriterContext(context.Background(), dir, maxSize)
}

func newPackWriterContext(ctx context.Context, dir string, maxSize int64) (*packWriter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("pack writer context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &packWriter{ctx: ctx, dir: dir, maxSize: maxSize}, nil
}

func (w *packWriter) openChunk() error {
	if w.pack != nil {
		return nil
	}
	pack, err := os.OpenFile(filepath.Join(w.dir, fmt.Sprintf("chunk-%06d.pack.tmp", w.chunk)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	index, err := os.OpenFile(filepath.Join(w.dir, fmt.Sprintf("chunk-%06d.idx.tmp", w.chunk)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = pack.Close()
		return err
	}
	w.pack, w.index = pack, index
	w.packBuffer, w.indexBuffer = bufio.NewWriterSize(pack, 1<<20), bufio.NewWriterSize(index, 1<<20)
	w.offset, w.records = 0, 0
	w.indexOffset = 0
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
	if _, err := w.packBuffer.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.packBuffer.Write(body); err != nil {
		return err
	}
	entry := indexRecord{
		Ordinal: record.Ordinal, Height: record.Height, Offset: start, Length: uint64(framedSize), Key: record.Key,
		OldSHA256: hex.EncodeToString(record.OldSHA256[:]), NewSHA256: hex.EncodeToString(record.NewSHA256[:]),
		SlotsAdded: record.SlotsAdded, SlotsRemoved: record.SlotsRemoved, SlotsChanged: record.SlotsChanged,
	}
	indexBody, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := w.indexBuffer.Write(append(indexBody, '\n')); err != nil {
		return err
	}
	w.offset += framedSize
	w.indexOffset += int64(len(indexBody) + 1)
	w.records++
	return nil
}

func (w *packWriter) Sync() (packWriterState, error) {
	if w.finalized {
		return packWriterState{}, fmt.Errorf("pack writer is finalized")
	}
	if w.pack != nil {
		if err := w.packBuffer.Flush(); err != nil {
			return packWriterState{}, err
		}
		if err := w.indexBuffer.Flush(); err != nil {
			return packWriterState{}, err
		}
		if err := w.pack.Sync(); err != nil {
			return packWriterState{}, err
		}
		if err := w.index.Sync(); err != nil {
			return packWriterState{}, err
		}
	}
	return packWriterState{
		Chunk: w.chunk, PackOffset: w.offset, IndexOffset: w.indexOffset, Records: w.records,
		Chunks: append([]chunkManifest(nil), w.chunks...),
	}, nil
}

func (w *packWriter) closeChunk() error {
	if w.pack == nil {
		return nil
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if err := w.packBuffer.Flush(); err != nil {
		return err
	}
	if err := w.indexBuffer.Flush(); err != nil {
		return err
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
	if err := commitFileNoReplace(packTmp, packPath); err != nil {
		return err
	}
	if err := commitFileNoReplace(indexTmp, indexPath); err != nil {
		return err
	}
	packHash, packSize, err := hashFileContext(w.ctx, packPath)
	if err != nil {
		return err
	}
	indexHash, _, err := hashFileContext(w.ctx, indexPath)
	if err != nil {
		return err
	}
	w.chunks = append(w.chunks, chunkManifest{
		Number: w.chunk, Pack: packName, PackSHA256: packHash, PackSize: packSize,
		Index: indexName, IndexSHA256: indexHash, Records: w.records,
	})
	w.chunk++
	w.pack, w.index = nil, nil
	w.packBuffer, w.indexBuffer = nil, nil
	w.offset, w.indexOffset, w.records = 0, 0, 0
	return syncDir(w.dir)
}

func (w *packWriter) Close() ([]chunkManifest, error) {
	if w.finalized {
		return nil, fmt.Errorf("pack writer already finalized")
	}
	if err := w.closeChunk(); err != nil {
		return nil, err
	}
	w.finalized = true
	return append([]chunkManifest(nil), w.chunks...), nil
}

func (w *packWriter) Abort() error {
	if w.finalized {
		return nil
	}
	w.finalized = true
	var errs []error
	if w.packBuffer != nil {
		errs = append(errs, w.packBuffer.Flush())
	}
	if w.indexBuffer != nil {
		errs = append(errs, w.indexBuffer.Flush())
	}
	if w.pack != nil {
		errs = append(errs, w.pack.Close())
	}
	if w.index != nil {
		errs = append(errs, w.index.Close())
	}
	return errors.Join(errs...)
}

func resumePackWriter(dir string, maxSize int64, state packWriterState, manifest planManifest) (*packWriter, error) {
	return resumePackWriterContext(context.Background(), dir, maxSize, state, manifest)
}

func resumePackWriterContext(ctx context.Context, dir string, maxSize int64, state packWriterState, manifest planManifest) (*packWriter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("resume pack writer context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if state.Chunk != len(state.Chunks) || state.Chunk < 0 || state.PackOffset < 0 || state.IndexOffset < 0 || state.Records < 0 {
		return nil, fmt.Errorf("invalid pack writer checkpoint")
	}
	var completedRecords int64
	for number, chunk := range state.Chunks {
		if chunk.Number != number || chunk.Records <= 0 {
			return nil, fmt.Errorf("invalid completed chunk %d", number)
		}
		packPath := filepath.Join(dir, chunk.Pack)
		indexPath := filepath.Join(dir, chunk.Index)
		if err := requireRegularFile(packPath, fmt.Sprintf("completed pack chunk %d", number)); err != nil {
			return nil, err
		}
		if err := requireRegularFile(indexPath, fmt.Sprintf("completed pack index %d", number)); err != nil {
			return nil, err
		}
		packHash, packSize, err := hashFileContext(ctx, packPath)
		if err != nil {
			return nil, err
		}
		indexHash, _, err := hashFileContext(ctx, indexPath)
		if err != nil {
			return nil, err
		}
		if packHash != chunk.PackSHA256 || packSize != chunk.PackSize || indexHash != chunk.IndexSHA256 {
			return nil, fmt.Errorf("completed pack chunk %d changed", number)
		}
		completedRecords += chunk.Records
	}
	if completedRecords+state.Records != manifest.Changed {
		return nil, fmt.Errorf("pack checkpoint has %d records, manifest has %d", completedRecords+state.Records, manifest.Changed)
	}
	if state.Records > 0 {
		type recovery struct {
			partial string
			final   string
			state   noReplacePartialState
		}
		recoveries := make([]recovery, 0, 2)
		for _, suffix := range []string{"pack", "idx"} {
			tmpPath := filepath.Join(dir, fmt.Sprintf("chunk-%06d.%s.tmp", state.Chunk, suffix))
			finalPath := filepath.Join(dir, fmt.Sprintf("chunk-%06d.%s", state.Chunk, suffix))
			recoveryState, err := inspectNoReplacePartial(tmpPath, finalPath, "resumable pack artifact")
			if err != nil {
				return nil, err
			}
			if recoveryState == noReplaceMissing {
				return nil, fmt.Errorf("resumable pack artifact is missing: %s", tmpPath)
			}
			recoveries = append(recoveries, recovery{partial: tmpPath, final: finalPath, state: recoveryState})
		}
		for _, artifact := range recoveries {
			if err := restoreNoReplacePartialState(artifact.partial, artifact.final, artifact.state); err != nil {
				return nil, err
			}
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		number, suffix, ok := parseChunkArtifact(entry.Name())
		if !ok {
			continue
		}
		if number < state.Chunk {
			if strings.HasSuffix(suffix, ".tmp") {
				return nil, fmt.Errorf("completed pack chunk %d has a stale partial artifact", number)
			}
			continue
		}
		keepCurrent := number == state.Chunk && state.Records > 0 && (suffix == ".pack.tmp" || suffix == ".idx.tmp")
		if keepCurrent {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return nil, err
		}
	}

	writer := &packWriter{
		ctx: ctx, dir: dir, maxSize: maxSize, chunk: state.Chunk, offset: state.PackOffset,
		indexOffset: state.IndexOffset, records: state.Records,
		chunks: append([]chunkManifest(nil), state.Chunks...),
	}
	var packPath, indexPath string
	if state.Records == 0 {
		if state.PackOffset != 0 || state.IndexOffset != 0 {
			return nil, fmt.Errorf("empty current pack chunk has non-zero offsets")
		}
	} else {
		packPath = filepath.Join(dir, fmt.Sprintf("chunk-%06d.pack.tmp", state.Chunk))
		indexPath = filepath.Join(dir, fmt.Sprintf("chunk-%06d.idx.tmp", state.Chunk))
		if err := truncateCheckpointFile(packPath, state.PackOffset); err != nil {
			return nil, err
		}
		if err := truncateCheckpointFile(indexPath, state.IndexOffset); err != nil {
			return nil, err
		}
	}
	validationManifest := manifest
	validationManifest.Chunks = append([]chunkManifest(nil), state.Chunks...)
	if state.Records > 0 {
		validationManifest.Chunks = append(validationManifest.Chunks, chunkManifest{
			Number: state.Chunk, Pack: filepath.Base(packPath), Index: filepath.Base(indexPath), Records: state.Records,
		})
	}
	if err := validateResumablePackContext(ctx, dir, validationManifest); err != nil {
		return nil, err
	}
	if state.Records == 0 {
		return writer, nil
	}
	pack, err := os.OpenFile(packPath, os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	index, err := os.OpenFile(indexPath, os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		_ = pack.Close()
		return nil, err
	}
	writer.pack, writer.index = pack, index
	writer.packBuffer, writer.indexBuffer = bufio.NewWriterSize(pack, 1<<20), bufio.NewWriterSize(index, 1<<20)
	return writer, nil
}

func validateResumablePackContext(ctx context.Context, dir string, manifest planManifest) error {
	if ctx == nil {
		return fmt.Errorf("validate resumable pack context is required")
	}
	stream := newPackStream(dir, manifest)
	var slotsAdded, slotsRemoved, slotsChanged uint64
	var oldBytes, newBytes, changedCanonical, changedConflict int64
	for {
		if err := ctx.Err(); err != nil {
			_ = stream.Close()
			return err
		}
		record, ok, err := stream.Next()
		if err != nil {
			_ = stream.Close()
			return fmt.Errorf("validate resumable pack: %w", err)
		}
		if !ok {
			break
		}
		if slotsAdded+record.SlotsAdded < slotsAdded || slotsRemoved+record.SlotsRemoved < slotsRemoved ||
			slotsChanged+record.SlotsChanged < slotsChanged {
			_ = stream.Close()
			return fmt.Errorf("validate resumable pack: slot counters overflow")
		}
		slotsAdded += record.SlotsAdded
		slotsRemoved += record.SlotsRemoved
		slotsChanged += record.SlotsChanged
		oldBytes += int64(len(record.OldBody))
		newBytes += int64(len(record.NewBody))
		if record.NoncanonicalOld {
			changedCanonical++
		}
		if record.ConflictingOld {
			changedConflict++
		}
	}
	if err := stream.Close(); err != nil {
		return err
	}
	if manifest.SlotsAdded != slotsAdded || manifest.SlotsRemoved != slotsRemoved || manifest.SlotsChanged != slotsChanged ||
		manifest.NewBytes != newBytes || manifest.ChangedCanonical != changedCanonical || manifest.ChangedConflict != changedConflict {
		return fmt.Errorf("resumable pack statistics differ from checkpoint manifest")
	}
	if manifest.OldBytes < oldBytes {
		return fmt.Errorf("resumable pack old bodies have %d bytes, checkpoint manifest has %d", oldBytes, manifest.OldBytes)
	}
	return nil
}

func truncateCheckpointFile(path string, size int64) error {
	if size < 0 {
		return fmt.Errorf("checkpoint file %s has a negative truncate size", path)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() || stat.Size() < size {
		return fmt.Errorf("checkpoint file %s is shorter than %d", path, size)
	}
	identity, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || identity.Nlink != 1 {
		return fmt.Errorf("checkpoint file %s must have exactly one link", path)
	}
	return file.Truncate(size)
}

func parseChunkArtifact(name string) (int, string, bool) {
	if !strings.HasPrefix(name, "chunk-") || len(name) < len("chunk-000000.pack") {
		return 0, "", false
	}
	number, err := strconv.Atoi(name[len("chunk-") : len("chunk-")+6])
	if err != nil {
		return 0, "", false
	}
	suffix := name[len("chunk-")+6:]
	switch suffix {
	case ".pack", ".idx", ".pack.tmp", ".idx.tmp":
		return number, suffix, true
	default:
		return 0, "", false
	}
}

func hashFile(path string) (string, int64, error) {
	return hashFileContext(context.Background(), path)
}

func hashFileContext(ctx context.Context, path string) (string, int64, error) {
	if ctx == nil {
		return "", 0, fmt.Errorf("hash file context is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hasher := sha256.New()
	buffer := make([]byte, 1<<20)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", 0, err
		}
		n, readErr := file.Read(buffer)
		if n > 0 {
			if _, err := hasher.Write(buffer[:n]); err != nil {
				return "", 0, err
			}
			total += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", 0, readErr
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)), total, nil
}

func iteratePack(dir string, manifest planManifest, fn func(packRecord) error) error {
	stream := newPackStream(dir, manifest)
	defer stream.Close()
	for {
		record, ok, err := stream.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := fn(record); err != nil {
			return err
		}
	}
}
