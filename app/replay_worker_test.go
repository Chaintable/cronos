package app

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/log"
	tmstate "github.com/tendermint/tendermint/proto/tendermint/state"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tharsis/ethermint/debank/replay"
)

// fakeWorker answers each connection with handle's result or error.
func fakeWorker(t *testing.T, handle func(replay.Request) (*replay.Result, error)) *replayProcess {
	t.Helper()
	dir, err := ioutil.TempDir("", "replay-test-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "w.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				req, err := replay.ReadRequest(conn)
				if err != nil {
					return
				}
				res, err := handle(req)
				if err != nil {
					_ = replay.WriteError(conn, err)
					return
				}
				_ = replay.WriteResult(conn, *res)
			}()
		}
	}()
	return &replayProcess{socket: socket, cmd: &exec.Cmd{}, exited: make(chan struct{})}
}

func request(height int64) replay.Request {
	return replay.Request{ChainID: ReplayChainID, ParentHash: bytes.Repeat([]byte{1}, 32), BeginBlock: &abci.RequestBeginBlock{Header: tmproto.Header{Height: height}}}
}

func TestReplayWorkerCall(t *testing.T) {
	appHash := bytes.Repeat([]byte{7}, 32)
	p := fakeWorker(t, func(req replay.Request) (*replay.Result, error) {
		if req.BeginBlock.Header.Height != 6346 {
			return nil, errors.New("parent commit mismatch")
		}
		return &replay.Result{
			Responses:    &tmstate.ABCIResponses{BeginBlock: &abci.ResponseBeginBlock{}, EndBlock: &abci.ResponseEndBlock{}},
			AppHash:      appHash,
			Transactions: []replay.Transaction{{CosmosIndex: 2}},
		}, nil
	})
	res, err := p.call(context.Background(), "", log.NewNopLogger(), request(6346))
	require.NoError(t, err)
	require.Equal(t, appHash, res.AppHash)
	require.Equal(t, 2, res.Transactions[0].CosmosIndex)

	_, err = p.call(context.Background(), "", log.NewNopLogger(), request(6347))
	require.EqualError(t, err, "parent commit mismatch")
}

func TestReplayWorkerCallCancelled(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	p := fakeWorker(t, func(replay.Request) (*replay.Result, error) {
		<-release
		return nil, errors.New("unreachable")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.call(ctx, "", log.NewNopLogger(), request(2))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReplayExecutorChecks(t *testing.T) {
	e := NewReplayExecutor("", log.NewNopLogger())
	require.NoError(t, e.CheckReplayHeight(ReplayChainID, 1))
	require.NoError(t, e.CheckReplayHeight(ReplayChainID, ReplayLastHeight))
	require.Error(t, e.CheckReplayHeight(ReplayChainID, ReplayLastHeight+1))
	require.Error(t, e.CheckReplayHeight("cronos_777-1", 10))

	for mode, want := range map[string]bool{"": false, "full": false, "blockfile": true} {
		t.Setenv("CRONOS_REPLAY_OUTPUT", mode)
		got, err := e.BlockFileOnly()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	t.Setenv("CRONOS_REPLAY_OUTPUT", "bad")
	_, err := e.BlockFileOnly()
	require.Error(t, err)
}
