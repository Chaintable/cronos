package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"cosmossdk.io/log/v2"
	"github.com/bytedance/sonic"
	"github.com/evmos/ethermint/debank/replay"
)

// replayProcess owns the long-lived `cronosd replay-worker` child. It is
// started on first use and restarted after it exits or a request times out.
type replayProcess struct {
	mu     sync.Mutex
	socket string
	cmd    *exec.Cmd
	stdin  io.Closer
	exited chan struct{}
}

func (p *replayProcess) call(ctx context.Context, home string, logger log.Logger, req replay.Request) (*replay.Result, error) {
	timeout, err := replayDurationEnv("CRONOS_REPLAY_TIMEOUT_SECONDS", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	socket, err := p.ensure(home, logger)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("connect replay worker: %w", err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if err = sonic.ConfigStd.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("send replay request: %w", err)
	}
	res, err := replay.ReadResult(conn)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// A request that outlives its deadline may be wedged; restart the worker.
		p.kill()
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return res, err
}

// ensure returns the socket of a running worker, starting one if needed.
func (p *replayProcess) ensure(home string, logger log.Logger) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		select {
		case <-p.exited:
		default:
			return p.socket, nil
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp("", "cronos-replay-worker-")
	if err != nil {
		return "", err
	}
	socket := filepath.Join(dir, "replay.sock")
	cmd := exec.Command(exe, "replay-worker", home, socket)
	cmd.Env = append(os.Environ(), "TMPDIR="+dir)
	// The worker exits when this pipe closes, i.e. when the writer exits.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	if err = cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return "", err
	}
	exited := make(chan struct{})
	go logReplayWorker(stderr, logger)
	go func() {
		err := cmd.Wait()
		logger.Info("replay worker exited", "error", err)
		os.RemoveAll(dir)
		close(exited)
	}()
	p.cmd, p.stdin, p.socket, p.exited = cmd, stdin, socket, exited
	if err = waitReplayWorker(socket, exited, 2*time.Minute); err != nil {
		_ = cmd.Process.Kill()
		return "", err
	}
	return socket, nil
}

func (p *replayProcess) kill() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
}

func waitReplayWorker(socket string, exited <-chan struct{}, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if conn, err := net.Dial("unix", socket); err == nil {
			return conn.Close()
		}
		select {
		case <-exited:
			return fmt.Errorf("replay worker exited during startup")
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replay worker not ready after %s", timeout)
		}
	}
}

func logReplayWorker(r io.Reader, logger log.Logger) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "replay worker timing") {
			logger.Debug(line)
		} else {
			logger.Error("replay worker", "output", line)
		}
	}
}

func replayDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw, set := os.LookupEnv(key)
	if !set {
		return fallback, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 1 {
		return 0, fmt.Errorf("invalid %s=%q: expected integer >= 1", key, raw)
	}
	return time.Duration(seconds) * time.Second, nil
}
