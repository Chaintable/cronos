//go:build !rocksdb

package cmd

import (
	"errors"
	"io"
)

func RunReplayWorker(_ string, _ io.Reader, _ io.Writer) error {
	return errors.New("historical replay requires read-only RocksDB support")
}
