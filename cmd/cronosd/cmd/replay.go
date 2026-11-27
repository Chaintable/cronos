//go:build rocksdb

package cmd

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/crypto-org-chain/cronos/v2/cmd/cronosd/opendb"
)

// RunReplayWorker serves replay requests on a unix socket until stdin closes,
// which happens when the parent process exits. One secondary database handle
// and its block cache are shared; every request builds a fresh app over it,
// because a sealed BaseApp cannot be reloaded at another height without Commit.
func RunReplayWorker(home, socket string, stdin io.Reader, stderr io.Writer) error {
	workers, err := replayIntEnv("CRONOS_REPLAY_WORKERS", 4, 1)
	if err != nil {
		return err
	}
	cacheMB, err := replayIntEnv("CRONOS_REPLAY_BLOCK_CACHE_MB", 1024, 8)
	if err != nil {
		return err
	}
	// A static archive has nothing to catch up with; skip it for backfills.
	catchUpEnabled, err := replayBoolEnv("CRONOS_REPLAY_CATCH_UP", true)
	if err != nil {
		return err
	}
	// Each request builds a fresh app, so the worker trades memory for less GC.
	gogc, err := replayIntEnv("CRONOS_REPLAY_GOGC", 400, 1)
	if err != nil {
		return err
	}
	debug.SetGCPercent(gogc)
	scratch, err := os.MkdirTemp("", "cronos-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	useFilter, err := replayBoolEnv("CRONOS_REPLAY_FILTER", false)
	if err != nil {
		return err
	}
	db, catchUp, err := opendb.OpenReplayDB(home, filepath.Join(scratch, "secondary"), uint64(cacheMB)<<20, useFilter)
	if err != nil {
		return fmt.Errorf("open parent application.db: %w", err)
	}
	defer db.Close()
	db = sharedDB{db}
	nodeCacheMB, err := replayIntEnv("CRONOS_REPLAY_NODE_CACHE_MB", 0, 0)
	if err != nil {
		return err
	}
	if nodeCacheMB > 0 {
		// Version/nonce keys are stable only within one archive view.
		if catchUpEnabled {
			return fmt.Errorf("CRONOS_REPLAY_NODE_CACHE_MB requires CRONOS_REPLAY_CATCH_UP=false")
		}
		nodes, err := newReplayNodeCache(db, nodeCacheMB)
		if err != nil {
			return err
		}
		db = nodes
		defer nodes.cache.Reset()
		defer nodes.report(stderr)
	}
	app.SetConfig()

	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() {
		_, _ = io.Copy(io.Discard, stdin)
		listener.Close()
	}()

	if !catchUpEnabled {
		catchUp = nil
	}
	timeout, err := replayIntEnv("CRONOS_REPLAY_TIMEOUT_SECONDS", 600, 1)
	if err != nil {
		return err
	}
	w := &replayWorker{
		db: db, catchUp: catchUp, scratch: scratch, slots: make(chan struct{}, workers), log: stderr,
		sessions: newReplaySessions(workers, time.Duration(timeout)*time.Second),
	}
	defer w.sessions.close()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			w.serve(conn)
		}()
	}
}
