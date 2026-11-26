//go:build replay
// +build replay

package cmd

import (
	"context"
	"fmt"
	"github.com/evmos/ethermint/debank/replay"
	rpcTypes "github.com/evmos/ethermint/rpc/types"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/libs/log"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/evmos/ethermint/debank/archive"
	"github.com/evmos/ethermint/rpc/namespaces/ethereum/trace"
	ethermint "github.com/evmos/ethermint/types"

	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/crypto-org-chain/cronos/v2/cmd/cronosd/opendb"
)

// RunReplayServer serves trace_debankBlock, eth_blockNumber and eth_chainId
// for the stopped archive in home/data until SIGINT or SIGTERM. No node of
// this version can start on the archive, whose latest state is newer.
func RunReplayServer(home, listen string) error {
	level, err := log.AllowLevel(os.Getenv("CRONOS_REPLAY_LOG_LEVEL"))
	if err != nil {
		level, _ = log.AllowLevel("info")
	}
	logger := log.NewFilter(log.NewTMLogger(log.NewSyncWriter(os.Stdout)), level)
	app.SetConfig()

	dbs, err := openReplayDatabases(home)
	if err != nil {
		return err
	}
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()
	chain := archive.New(dbs[0], dbs[1], dbs[2], app.MakeEncodingConfig().Codec)
	chainID, err := ethermint.ParseChainID(app.ReplayChainID)
	if err != nil {
		return err
	}
	txDecoder := app.MakeEncodingConfig().TxConfig.TxDecoder()
	executor := app.NewReplayExecutor(home, logger)

	server := gethrpc.NewServer()
	if err := server.RegisterName("trace", trace.NewAPI(logger, chain, txDecoder, chainID, executor)); err != nil {
		return err
	}
	if err := server.RegisterName("eth", trace.NewEthAPI(chain, chainID)); err != nil {
		return err
	}
	httpServer := &http.Server{Addr: listen, Handler: server}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- httpServer.ListenAndServe() }()
	logger.Info("replay server listening", "address", listen, "head", chain.Head())
	select {
	case err := <-errc:
		return fmt.Errorf("replay server: %w", err)
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdown)
}

func openReplayDatabases(home string) ([]dbm.DB, error) {
	data := filepath.Join(home, "data")
	maxOpenFiles, err := replayIntEnv("CRONOS_REPLAY_MAX_OPEN_FILES", 1024, 1)
	if err != nil {
		return nil, err
	}
	// Each RPC database has a separate cache. Honor smaller worker cache
	// budgets while keeping the existing 256 MiB ceiling per database.
	cacheMB, err := replayIntEnv("CRONOS_REPLAY_BLOCK_CACHE_MB", 256, 8)
	if err != nil {
		return nil, err
	}
	if cacheMB > 256 {
		cacheMB = 256
	}
	var dbs []dbm.DB
	open := func(name string) error {
		db, err := opendb.OpenReadOnly(data, name, uint64(cacheMB)<<20, maxOpenFiles, false)
		if err == nil {
			dbs = append(dbs, db)
		}
		return err
	}
	for _, name := range []string{"blockstore", "state", "application"} {
		if err := open(name); err != nil {
			for _, db := range dbs {
				db.Close()
			}
			return nil, err
		}
	}
	return dbs, nil
}

type localReplay struct {
	*app.ReplayExecutor
	db      dbm.DB
	scratch string
}

func (e localReplay) ReplayBlock(ctx context.Context, req replay.Request) (*replay.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return app.ReplayOnDB(e.db, e.scratch, req, func(string) {})
}

// RunReplayHeight forces a native replay against the archive, including empty EVM blocks.
func RunReplayHeight(home, rawHeight string, out io.Writer) error {
	height, err := strconv.ParseInt(rawHeight, 10, 64)
	if err != nil {
		return err
	}
	app.SetConfig()
	logger := log.NewNopLogger()
	dbs, err := openReplayDatabases(home)
	if err != nil {
		return err
	}
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()
	scratch, err := os.MkdirTemp("", "cronos-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	id, err := ethermint.ParseChainID(app.ReplayChainID)
	if err != nil {
		return err
	}
	chain := archive.New(dbs[0], dbs[1], dbs[2], app.MakeEncodingConfig().Codec)
	executor := localReplay{app.NewReplayExecutor(home, logger), dbs[2], scratch}
	api := trace.NewAPI(logger, chain, app.MakeEncodingConfig().TxConfig.TxDecoder(), id, executor)
	result, err := trace.ReplayHeight(context.Background(), api, rpcTypes.BlockNumber(height))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(result))
	return err
}
