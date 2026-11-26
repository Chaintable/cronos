//go:build replay
// +build replay

package cmd

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	dbm "github.com/cometbft/cometbft-db"

	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/crypto-org-chain/cronos/v2/cmd/cronosd/opendb"
	"github.com/evmos/ethermint/debank/replay"
)

// RunReplayWorker serves replay requests on a unix socket until stdin closes,
// which happens when the parent process exits. One read-only database handle
// and its block cache are shared by every request.
func RunReplayWorker(home, socket string, stdin io.Reader, stderr io.Writer) error {
	workers, err := replayIntEnv("CRONOS_REPLAY_WORKERS", 4, 1)
	if err != nil {
		return err
	}
	cacheMB, err := replayIntEnv("CRONOS_REPLAY_BLOCK_CACHE_MB", 1024, 8)
	if err != nil {
		return err
	}
	// A bounded table cache lets a small worker open a multi-terabyte archive.
	maxOpenFiles, err := replayIntEnv("CRONOS_REPLAY_MAX_OPEN_FILES", -1, 1)
	if err != nil {
		return err
	}
	// Each request builds a fresh app, so the worker trades memory for less GC.
	gogc, err := replayIntEnv("CRONOS_REPLAY_GOGC", 400, 1)
	if err != nil {
		return err
	}
	debug.SetGCPercent(gogc)
	scratch, err := ioutil.TempDir("", "cronos-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	var useFilter bool
	if raw, set := os.LookupEnv("CRONOS_REPLAY_FILTER"); set {
		useFilter, err = strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid CRONOS_REPLAY_FILTER=%q: expected a boolean", raw)
		}
	}
	db, err := opendb.OpenReadOnly(filepath.Join(home, "data"), "application", uint64(cacheMB)<<20, maxOpenFiles, useFilter)
	if err != nil {
		return fmt.Errorf("open parent application.db: %w", err)
	}
	defer db.Close()
	nodeCacheMB, err := replayIntEnv("CRONOS_REPLAY_NODE_CACHE_MB", 0, 0)
	if err != nil {
		return err
	}
	if nodeCacheMB > 0 {
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
		_, _ = io.Copy(ioutil.Discard, stdin)
		listener.Close()
	}()

	w := &replayWorker{db: db, scratch: scratch, slots: make(chan struct{}, workers), log: stderr}
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

type replayWorker struct {
	db      dbm.DB
	scratch string
	slots   chan struct{}
	log     io.Writer
	logMu   sync.Mutex
}

func (w *replayWorker) serve(conn net.Conn) {
	req, err := replay.ReadRequest(conn)
	if err != nil {
		_ = replay.WriteError(conn, err)
		return
	}
	w.slots <- struct{}{}
	defer func() { <-w.slots }()
	timing := newWorkerTiming()
	res, err := w.replay(req, timing)
	if err != nil {
		_ = replay.WriteError(conn, err)
		return
	}
	if err = replay.WriteResult(conn, *res); err != nil {
		return
	}
	timing.mark("write")
	w.logMu.Lock()
	timing.report(w.log, req.BeginBlock.Header.Height)
	if req.BeginBlock.Header.Height%1000 == 0 {
		if nodes, ok := w.db.(*replayNodeCache); ok {
			nodes.report(w.log)
		}
	}
	w.logMu.Unlock()
}

func (w *replayWorker) replay(req replay.Request, timing *workerTiming) (res *replay.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("replay panic: %v", r)
		}
	}()
	// Upgrade metadata and keeper scratch files belong to this request.
	home, err := ioutil.TempDir(w.scratch, "app-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(home)
	return app.ReplayOnDB(w.db, home, req, timing.mark)
}

func replayIntEnv(key string, fallback, minimum int) (int, error) {
	raw, set := os.LookupEnv(key)
	if !set {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum {
		return 0, fmt.Errorf("invalid %s=%q: expected integer >= %d", key, raw, minimum)
	}
	return value, nil
}

// workerTiming reports phase milliseconds on stderr for the parent's log.
type workerTiming struct {
	last  time.Time
	parts []string
}

func newWorkerTiming() *workerTiming { return &workerTiming{last: time.Now()} }

func (t *workerTiming) mark(name string) {
	now := time.Now()
	t.parts = append(t.parts, fmt.Sprintf("%s_ms=%d", name, now.Sub(t.last).Milliseconds()))
	t.last = now
}

func (t *workerTiming) report(w io.Writer, height int64) {
	fmt.Fprintln(w, "replay worker timing", "height="+strconv.FormatInt(height, 10), strings.Join(t.parts, " "))
}
