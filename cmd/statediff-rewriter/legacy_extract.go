package main

import (
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/cosmos/iavl"
)

type legacyDumpResult struct {
	Scan             legacyScanReport
	Scanned          bool
	GeneratedFiles   int64
	ReusedFiles      int64
	GeneratedRecords int64
	ReusedRecords    int64
}

func writeLegacyDumpRanges(
	ctx context.Context,
	source legacyRecordSource,
	scanOptions legacyScanOptions,
	evmDir string,
	ranges []versionRange,
	zlibLevel int,
	minFree uint64,
) (result legacyDumpResult, returnErr error) {
	if len(ranges) == 0 {
		return result, fmt.Errorf("legacy dump ranges are empty")
	}
	if ranges[0].First != scanOptions.FirstVersion || ranges[len(ranges)-1].Last != scanOptions.LastVersion {
		return result, fmt.Errorf(
			"legacy dump ranges cover %d-%d, scan covers %d-%d",
			ranges[0].First, ranges[len(ranges)-1].Last,
			scanOptions.FirstVersion, scanOptions.LastVersion,
		)
	}
	writer, err := newSequentialDumpWriterContext(ctx, evmDir, ranges, zlibLevel, minFree)
	if err != nil {
		return result, err
	}
	if writer.reusedFiles == int64(len(ranges)) {
		result.ReusedFiles = writer.reusedFiles
		result.ReusedRecords = writer.reusedRecords
		return result, nil
	}
	defer func() { returnErr = errors.Join(returnErr, writer.abort()) }()

	report, err := scanLegacyChangeSets(ctx, source, scanOptions, writer.write)
	if err != nil {
		return result, err
	}
	if err := writer.finish(); err != nil {
		return result, err
	}
	result.Scan = report
	result.Scanned = true
	result.GeneratedFiles = writer.generatedFiles
	result.ReusedFiles = writer.reusedFiles
	result.GeneratedRecords = writer.generatedRecords
	result.ReusedRecords = writer.reusedRecords
	return result, nil
}

type sequentialDumpWriter struct {
	evmDir           string
	ranges           []versionRange
	reused           []bool
	zlibLevel        int
	minFree          uint64
	space            *freeSpaceReserver
	rangeIndex       int
	expected         int64
	file             *os.File
	compressed       *zlib.Writer
	partialPath      string
	finalPath        string
	finished         bool
	generatedFiles   int64
	reusedFiles      int64
	generatedRecords int64
	reusedRecords    int64
}

func newSequentialDumpWriter(
	evmDir string,
	ranges []versionRange,
	zlibLevel int,
	minFree uint64,
) (*sequentialDumpWriter, error) {
	return newSequentialDumpWriterContext(context.Background(), evmDir, ranges, zlibLevel, minFree)
}

func newSequentialDumpWriterContext(
	ctx context.Context,
	evmDir string,
	ranges []versionRange,
	zlibLevel int,
	minFree uint64,
) (*sequentialDumpWriter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("sequential dump writer context is required")
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("sequential dump ranges are empty")
	}
	for index, blockRange := range ranges {
		if blockRange.First < 1 || blockRange.Last < blockRange.First {
			return nil, fmt.Errorf("invalid sequential dump range %d-%d", blockRange.First, blockRange.Last)
		}
		if index > 0 && blockRange.First != ranges[index-1].Last+1 {
			return nil, fmt.Errorf("non-contiguous sequential dump ranges at %d", blockRange.First)
		}
	}
	space, err := newFreeSpaceReserver(evmDir, minFree)
	if err != nil {
		return nil, err
	}
	writer := &sequentialDumpWriter{
		evmDir: evmDir, ranges: ranges, reused: make([]bool, len(ranges)),
		zlibLevel: zlibLevel, minFree: minFree, space: space, expected: ranges[0].First,
	}
	for index, blockRange := range ranges {
		path := dumpRangePath(evmDir, blockRange)
		if _, err := os.Lstat(path); err == nil {
			if err := validateReusableDumpRange(ctx, path, blockRange); err != nil {
				return nil, err
			}
			writer.reused[index] = true
			writer.reusedFiles++
			writer.reusedRecords += blockRange.Last - blockRange.First + 1
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return writer, nil
}

func (writer *sequentialDumpWriter) write(version int64, changeSet *iavl.ChangeSet) error {
	if writer.finished {
		return fmt.Errorf("sequential dump writer is finished")
	}
	if version != writer.expected {
		return fmt.Errorf("sequential dump received version %d, want %d", version, writer.expected)
	}
	if writer.rangeIndex >= len(writer.ranges) {
		return fmt.Errorf("sequential dump received version %d after final range", version)
	}
	blockRange := writer.ranges[writer.rangeIndex]
	if version < blockRange.First || version > blockRange.Last {
		return fmt.Errorf("sequential dump version %d is outside current range %d-%d", version, blockRange.First, blockRange.Last)
	}
	if !writer.reused[writer.rangeIndex] {
		if writer.file == nil {
			if version != blockRange.First {
				return fmt.Errorf("sequential dump opens range %d-%d at version %d", blockRange.First, blockRange.Last, version)
			}
			if err := writer.openRange(blockRange); err != nil {
				return err
			}
		}
		if err := writeChangeSet(writer.compressed, version, changeSet); err != nil {
			return err
		}
		writer.generatedRecords++
	}
	writer.expected++
	if version == blockRange.Last {
		if !writer.reused[writer.rangeIndex] {
			if err := writer.closeRange(); err != nil {
				return err
			}
			writer.generatedFiles++
		}
		writer.rangeIndex++
	}
	return nil
}

func (writer *sequentialDumpWriter) openRange(blockRange versionRange) error {
	if err := ensureFreeSpace(writer.evmDir, writer.minFree); err != nil {
		return err
	}
	writer.finalPath = dumpRangePath(writer.evmDir, blockRange)
	writer.partialPath = writer.finalPath + ".partial"
	file, err := openLockedPartial(writer.partialPath)
	if err != nil {
		return err
	}
	compressed, err := zlib.NewWriterLevel(reservedWriter{writer: file, reserver: writer.space}, writer.zlibLevel)
	if err != nil {
		_ = file.Close()
		return err
	}
	writer.file, writer.compressed = file, compressed
	return nil
}

func (writer *sequentialDumpWriter) closeRange() error {
	compressed, file := writer.compressed, writer.file
	writer.compressed, writer.file = nil, nil
	if compressed == nil || file == nil {
		return fmt.Errorf("sequential dump range is not open")
	}
	defer func() { _ = file.Close() }()
	if err := compressed.Close(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := commitFileNoReplace(writer.partialPath, writer.finalPath); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	writer.partialPath, writer.finalPath = "", ""
	return nil
}

func (writer *sequentialDumpWriter) finish() error {
	if writer.file != nil || writer.compressed != nil {
		return fmt.Errorf("sequential dump finished with an open range")
	}
	if writer.rangeIndex != len(writer.ranges) {
		return fmt.Errorf("sequential dump finished after %d of %d ranges", writer.rangeIndex, len(writer.ranges))
	}
	if writer.expected != writer.ranges[len(writer.ranges)-1].Last+1 {
		return fmt.Errorf("sequential dump finished at version %d", writer.expected-1)
	}
	writer.finished = true
	return nil
}

func (writer *sequentialDumpWriter) abort() error {
	if writer == nil || writer.finished {
		return nil
	}
	var errs []error
	if writer.compressed != nil {
		errs = append(errs, writer.compressed.Close())
		writer.compressed = nil
	}
	if writer.file != nil {
		errs = append(errs, writer.file.Close())
		writer.file = nil
	}
	return errors.Join(errs...)
}
