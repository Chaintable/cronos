package app

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tmstate "github.com/tendermint/tendermint/proto/tendermint/state"
	"github.com/tharsis/ethermint/debank/replay"
)

func TestReplaySegmentDecision(t *testing.T) {
	for _, prefix := range []string{"", "\n"} {
		require.NoError(t, ReadReplaySegmentDecision(strings.NewReader(prefix+"Y")))
		require.Equal(t, io.EOF, ReadReplaySegmentDecision(strings.NewReader(prefix)))
		require.Error(t, ReadReplaySegmentDecision(strings.NewReader(prefix+"X")))
	}
	require.Error(t, ReadReplaySegmentDecision(strings.NewReader("\n\nY")))
}

func TestSegmentConfirmationUsesExecutingConnection(t *testing.T) {
	for _, tc := range []struct {
		name              string
		verify, cancel    bool
		expectAffirmation bool
	}{
		{"confirmed", true, false, true},
		{"verification failed", false, false, false},
		{"cancelled after execution", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Short paths fit the platform's Unix socket address limit.
			dir, err := os.MkdirTemp("", "seg-")
			require.NoError(t, err)
			t.Cleanup(func() { os.RemoveAll(dir) })
			socket := filepath.Join(dir, "s")
			ln, err := net.Listen("unix", socket)
			require.NoError(t, err)
			defer ln.Close()
			executor := &ReplayExecutor{process: replayProcess{cmd: &exec.Cmd{}, socket: socket, exited: make(chan struct{})}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seg := replay.Segment{ID: "same-connection", Start: ReplayFirstHeight, End: ReplayFirstHeight + 1}
			done := make(chan error, 1)
			confirmed := make(chan bool, 1)
			go func() {
				ack, err := respondToSegment(ln, seg)
				confirmed <- ack
				done <- err
			}()
			_, finish, err := executor.ReplaySegmentBlock(ctx, request(seg.Start), seg)
			require.NoError(t, err)
			if tc.cancel {
				cancel()
			}
			err = finish(tc.verify)
			if tc.cancel {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("worker still waiting for confirmation")
			}
			require.Equal(t, tc.expectAffirmation, <-confirmed)
		})
	}
}

func respondToSegment(ln net.Listener, seg replay.Segment) (bool, error) {
	conn, err := ln.Accept()
	if err != nil {
		return false, err
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return false, err
	}
	if len(line) < 2 || line[0] != 'S' {
		return false, fmt.Errorf("expected segment header")
	}
	var header ReplaySegmentHeader
	if err = json.Unmarshal(line[1:], &header); err != nil {
		return false, err
	}
	if header.Segment != seg || header.Height != seg.Start || header.Drop {
		return false, fmt.Errorf("unexpected segment header: %+v", header)
	}
	if _, err = replay.ReadRequest(reader); err != nil {
		return false, err
	}
	if err = replay.WriteResult(conn, replay.Result{Responses: &tmstate.ABCIResponses{}}); err != nil {
		return false, err
	}
	err = ReadReplaySegmentDecision(reader)
	if err == io.EOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, WriteReplaySegmentAck(conn, nil)
}
