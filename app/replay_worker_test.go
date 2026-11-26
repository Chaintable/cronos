package app

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"cosmossdk.io/log"
	"github.com/bytedance/sonic"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/stretchr/testify/require"
)

// fakeWorker answers each connection with handle's result or error.
func fakeWorker(t *testing.T, handle func(replay.Request) (*replay.Result, error)) *replayProcess {
	t.Helper()
	dir, err := os.MkdirTemp("", "replay-test-")
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
				var req replay.Request
				if err := sonic.ConfigStd.NewDecoder(conn).Decode(&req); err != nil {
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

func TestReplayWorkerCall(t *testing.T) {
	appHash := bytes.Repeat([]byte{7}, 32)
	request := replay.Request{ChainID: "cronosmainnet_25-1", ParentHash: bytes.Repeat([]byte{1}, 32), Request: &abci.RequestFinalizeBlock{Height: 45062801}}
	p := fakeWorker(t, func(req replay.Request) (*replay.Result, error) {
		if req.Request.Height != 45062801 {
			return nil, errors.New("parent commit mismatch")
		}
		return &replay.Result{Finalize: &abci.ResponseFinalizeBlock{AppHash: appHash}, Transactions: []replay.Transaction{{CosmosIndex: 2}}}, nil
	})
	res, err := p.call(context.Background(), "", log.NewNopLogger(), request)
	require.NoError(t, err)
	require.Equal(t, appHash, res.Finalize.AppHash)
	require.Equal(t, 2, res.Transactions[0].CosmosIndex)

	request.Request = &abci.RequestFinalizeBlock{Height: 45062802}
	_, err = p.call(context.Background(), "", log.NewNopLogger(), request)
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
	_, err := p.call(ctx, "", log.NewNopLogger(), replay.Request{Request: &abci.RequestFinalizeBlock{Height: 2}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
