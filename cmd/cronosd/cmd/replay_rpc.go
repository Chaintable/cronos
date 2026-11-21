package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/server"
	"github.com/ethereum/go-ethereum/common"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	emrpc "github.com/evmos/ethermint/rpc"
	rt "github.com/evmos/ethermint/rpc/types"
	ethermint "github.com/evmos/ethermint/types"
	tmclient "github.com/tendermint/tendermint/rpc/client"
	core "github.com/tendermint/tendermint/rpc/core/types"
	ws "github.com/tendermint/tendermint/rpc/jsonrpc/client"
)

func init() {
	err := emrpc.RegisterAPINamespace("trace", func(sc *server.Context, cc client.Context, _ *ws.WSClient, _ bool, _ ethermint.EVMTxIndexer) []gethrpc.API {
		api := &historicalTrace{client: cc.Client, home: sc.Config.RootDir, archive: filepath.Join(sc.Config.RootDir, "replay", "archive"), source: sc.Config.RPC.ListenAddress, worker: make(chan struct{}, 1)}
		return []gethrpc.API{{Namespace: "trace", Version: "1.0", Service: api, Public: true}}
	})
	if err != nil {
		panic(err)
	}
}

type historicalTrace struct {
	client                tmclient.Client
	home, archive, source string
	worker                chan struct{}
}

func bundlePath(home string, height int64, hash common.Hash) string {
	return filepath.Join(home, "replay", "blocks", strconv.FormatInt(height/1000, 10), hash.Hex()+".json")
}
func saveBundle(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".replay-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
func checkBundle(data []byte, height int64, hash common.Hash) error {
	var id struct {
		Header    struct{ Hash common.Hash }
		BlockFile struct {
			Block struct {
				ID     string
				Height int64
			}
		} `json:"block_file"`
	}
	if err := json.Unmarshal(data, &id); err != nil {
		return err
	}
	if id.Header.Hash != hash || id.BlockFile.Block.ID != hash.Hex() || id.BlockFile.Block.Height != height {
		return fmt.Errorf("replay block identity mismatch at %d", height)
	}
	return nil
}
func (api *historicalTrace) DebankBlock(ctx context.Context, id rt.BlockNumberOrHash) (json.RawMessage, error) {
	var b *core.ResultBlock
	var err error
	switch {
	case id.BlockHash != nil:
		b, err = api.client.BlockByHash(ctx, id.BlockHash.Bytes())
	case id.BlockNumber == nil:
		return nil, fmt.Errorf("block number or hash is required")
	case *id.BlockNumber == rt.EthLatestBlockNumber:
		b, err = api.client.Block(ctx, nil)
	default:
		h := int64(*id.BlockNumber)

		if h < 6542800 || h > 11608759 {
			return nil, fmt.Errorf("unsupported replay height %d", h)
		}
		b, err = api.client.Block(ctx, &h)
	}
	if err != nil {
		return nil, err
	}
	if b == nil || b.Block == nil || b.Block.ChainID != "cronosmainnet_25-1" || b.Block.Height < 6542800 || b.Block.Height > 11608759 {
		return nil, fmt.Errorf("block outside Cronos v1.0.2 mainnet interval")
	}
	h := b.Block.Height
	hash := common.BytesToHash(b.Block.Hash())
	if id.BlockHash != nil && *id.BlockHash != hash {
		return nil, fmt.Errorf("archive returned different block hash")
	}
	path := bundlePath(api.home, h, hash)
	read := func() (json.RawMessage, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return data, checkBundle(data, h, hash)
	}
	if data, err := read(); !os.IsNotExist(err) {
		return data, err
	}
	select {
	case api.worker <- struct{}{}:
		defer func() { <-api.worker }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if data, err := read(); !os.IsNotExist(err) {
		return data, err
	}
	temp, err := os.MkdirTemp("", "cronos-v102-replay-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(temp)
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, exe, "replay-blocks", api.source, strconv.FormatInt(h, 10), strconv.FormatInt(h, 10), "--home", temp, "--archive-state-dir", api.archive)
	var diag bytes.Buffer
	cmd.Stderr = &diag
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("replay block %d: %w: %s", h, err, diag.String())
	}
	if err = checkBundle(data, h, hash); err != nil {
		return nil, err
	}
	if err = saveBundle(path, data); err != nil {
		return nil, err
	}
	return data, nil
}
