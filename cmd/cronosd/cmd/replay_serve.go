//go:build replay
// +build replay

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/tendermint/tendermint/libs/log"
	dbm "github.com/tendermint/tm-db"
	"github.com/tharsis/ethermint/debank/archive"
	"github.com/tharsis/ethermint/rpc/ethereum/namespaces/trace"
	ethermint "github.com/tharsis/ethermint/types"

	"github.com/crypto-org-chain/cronos/app"
	"github.com/crypto-org-chain/cronos/cmd/cronosd/opendb"
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

	data := filepath.Join(home, "data")
	maxOpenFiles, err := replayIntEnv("CRONOS_REPLAY_MAX_OPEN_FILES", 1024, 1)
	if err != nil {
		return err
	}
	var dbs []dbm.DB
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()
	open := func(name string) error {
		db, err := opendb.OpenReadOnly(data, name, 256<<20, maxOpenFiles)
		if err == nil {
			dbs = append(dbs, db)
		}
		return err
	}
	for _, name := range []string{"blockstore", "state", "application"} {
		if err := open(name); err != nil {
			return err
		}
	}
	chain := archive.New(dbs[0], dbs[1], dbs[2])
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
