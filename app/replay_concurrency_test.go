package app

import (
	"encoding/json"
	"math/big"
	"runtime"
	"sync"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	tmtypes "github.com/cometbft/cometbft/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/ethereum/go-ethereum/common"
	eth "github.com/ethereum/go-ethereum/core/types"
	"github.com/evmos/ethermint/crypto/ethsecp256k1"
	"github.com/evmos/ethermint/debank/replay"
	"github.com/evmos/ethermint/tests"
	evm "github.com/evmos/ethermint/x/evm/types"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/log/v2"
	"cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/testutil/mock"
	sims "github.com/cosmos/cosmos-sdk/testutil/sims"
	sdk "github.com/cosmos/cosmos-sdk/types"
	auth "github.com/cosmos/cosmos-sdk/x/auth/types"
	bank "github.com/cosmos/cosmos-sdk/x/bank/types"
)

// A long-lived replay process builds a fresh app per request while other
// requests execute and decode transactions on the same database.
func TestConcurrentFreshAppReplay(t *testing.T) {
	db := dbm.NewMemDB()
	open := func(latest bool) *App {
		opts := MinimalOptionsMap{flags.FlagHome: t.TempDir(), FlagDisableOptimisticExecution: true}
		return New(log.NewNopLogger(), db, latest, opts, baseapp.SetChainID(TestAppChainID), baseapp.SetIAVLDisableFastNode(true))
	}
	a := open(true)
	denom := evm.DefaultEVMDenom
	var keys []*ethsecp256k1.PrivKey
	var accounts []auth.GenesisAccount
	var balances []bank.Balance
	for i := 0; i < 3; i++ {
		key, err := ethsecp256k1.GenerateKey()
		require.NoError(t, err)
		addr := sdk.AccAddress(key.PubKey().Address())
		keys = append(keys, key)
		accounts = append(accounts, auth.NewBaseAccount(addr, key.PubKey(), uint64(i), 0))
		balances = append(balances, bank.Balance{Address: addr.String(), Coins: sdk.NewCoins(sdk.NewCoin(denom, math.NewIntWithDecimal(10, 18)))})
	}
	pub, err := mock.NewPV().GetPubKey()
	require.NoError(t, err)
	val := tmtypes.NewValidator(pub, 1)
	g, err := sims.GenesisStateWithValSet(a.AppCodec(), a.DefaultGenesis(), tmtypes.NewValidatorSet([]*tmtypes.Validator{val}), accounts, balances...)
	require.NoError(t, err)
	bz, err := json.Marshal(g)
	require.NoError(t, err)
	_, err = a.InitChain(&abci.RequestInitChain{ChainId: TestAppChainID, AppStateBytes: bz, ConsensusParams: DefaultConsensusParams})
	require.NoError(t, err)
	finalize := func(h int64, txs ...[]byte) *abci.RequestFinalizeBlock {
		return &abci.RequestFinalizeBlock{Height: h, Time: time.Unix(h, 0).UTC(), ProposerAddress: val.Address, Txs: txs}
	}
	_, err = a.FinalizeBlock(finalize(1))
	require.NoError(t, err)
	_, err = a.Commit()
	require.NoError(t, err)

	// Each sender deploys a contract that calls the identity precompile and emits LOG0.
	code := common.FromHex("0x6000600060006000600073" + "0000000000000000000000000000000000000004" + "5af15060006000a000")
	var txs [][]byte
	for i, key := range keys {
		tx := evm.NewTx(TestEthChainID, 0, nil, big.NewInt(0), 300000, nil, big.NewInt(100000000000), big.NewInt(0), code, nil)
		tx.From = common.BytesToAddress(key.PubKey().Address()).Bytes()
		require.NoError(t, tx.Sign(eth.LatestSignerForChainID(TestEthChainID), tests.NewSigner(keys[i])))
		signed, err := tx.BuildTx(a.TxConfig().NewTxBuilder(), denom)
		require.NoError(t, err)
		raw, err := a.TxConfig().TxEncoder()(signed)
		require.NoError(t, err)
		txs = append(txs, raw)
	}
	req := finalize(2, txs...)

	run := func() (*abci.ResponseFinalizeBlock, []replay.Transaction) {
		opts := MinimalOptionsMap{flags.FlagHome: t.TempDir(), FlagDisableOptimisticExecution: true}
		replica := New(log.NewNopLogger(), db, false, opts, ReplayAppOptions(TestAppChainID)...)
		require.NoError(t, replica.LoadHeight(1))
		observed := ObserveReplayTransactions(replica.EvmKeeper)
		out, err := replica.FinalizeBlock(req)
		require.NoError(t, err)
		return out, *observed
	}
	want, wantTxs := run()
	require.Len(t, wantTxs, len(keys))
	for _, r := range want.TxResults {
		require.Zero(t, r.Code, r.Log)
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, gotTxs := run()
			require.Equal(t, want, got)
			require.Equal(t, wantTxs, gotTxs)
		}()
	}
	wg.Wait()

	// Discarded replay apps must not leave goroutines behind.
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		run()
	}
	runtime.GC()
	require.LessOrEqual(t, runtime.NumGoroutine(), before+2)
}
