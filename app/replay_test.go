package app

import (
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"sync"
	"testing"
	"time"

	sdkmath "cosmossdk.io/math"
	dbm "github.com/cometbft/cometbft-db"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/libs/log"
	tmproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cosmos/cosmos-sdk/baseapp"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/evmos/ethermint/crypto/ethsecp256k1"
	"github.com/evmos/ethermint/debank/replay"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/evmos/ethermint/tests"
	evmtypes "github.com/evmos/ethermint/x/evm/types"
	feemarkettypes "github.com/evmos/ethermint/x/feemarket/types"
	"github.com/stretchr/testify/require"
)

func dbDigest(t *testing.T, db dbm.DB) [32]byte {
	it, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	defer it.Close()
	h := sha256.New()
	for ; it.Valid(); it.Next() {
		h.Write(it.Key())
		h.Write(it.Value())
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

type replayFixture struct {
	db       dbm.DB
	req      replay.Request
	want     []abci.ResponseDeliverTx
	wantHash []byte
}

// newReplayFixture executes block 2, in which three senders each deploy a
// contract that calls the identity precompile and emits LOG0, under a block
// gas limit of maxGas.
func newReplayFixture(t *testing.T, maxGas int64) replayFixture {
	db := dbm.NewMemDB()
	enc := MakeEncodingConfig()
	a := New(log.NewNopLogger(), db, nil, true, true, map[int64]bool{}, t.TempDir(), 0, enc, EmptyAppOptions{}, baseapp.SetChainID(TestAppChainID))
	denom := evmtypes.DefaultEVMDenom
	ethChainID := big.NewInt(777)

	valKey := ed25519.GenPrivKey()
	valConsAddr := sdk.ConsAddress(valKey.PubKey().Address())
	pk, err := cryptocodec.FromTmPubKeyInterface(valKey.PubKey())
	require.NoError(t, err)
	pkAny, err := codectypes.NewAnyWithValue(pk)
	require.NoError(t, err)

	var keys []*ethsecp256k1.PrivKey
	var accounts []authtypes.GenesisAccount
	var balances []banktypes.Balance
	supply := sdk.NewCoins()
	for i := 0; i < 3; i++ {
		key, err := ethsecp256k1.GenerateKey()
		require.NoError(t, err)
		addr := sdk.AccAddress(key.PubKey().Address())
		keys = append(keys, key)
		accounts = append(accounts, authtypes.NewBaseAccount(addr, nil, uint64(i), 0))
		coins := sdk.NewCoins(sdk.NewCoin(denom, sdkmath.NewIntWithDecimal(10, 18)))
		balances = append(balances, banktypes.Balance{Address: addr.String(), Coins: coins})
		supply = supply.Add(coins...)
	}
	bond := sdkmath.NewInt(1000000)
	balances = append(balances, banktypes.Balance{Address: authtypes.NewModuleAddress(stakingtypes.BondedPoolName).String(), Coins: sdk.NewCoins(sdk.NewCoin(sdk.DefaultBondDenom, bond))})
	supply = supply.Add(sdk.NewCoin(sdk.DefaultBondDenom, bond))

	genesis := NewDefaultGenesisState(enc.Codec)
	// Mainnet ran these extra EIPs until v1.0; concurrent EVMs must not share
	// the instruction set they rewrite.
	evmGenesis := evmtypes.DefaultGenesisState()
	evmGenesis.Params.ExtraEIPs = []int64{2929, 2200, 1884, 1344}
	genesis[evmtypes.ModuleName] = enc.Codec.MustMarshalJSON(evmGenesis)
	// Each transaction is charged its whole gas limit, so a block gas limit
	// can admit one transaction, which the ante handler requires, but not two.
	feemarketGenesis := feemarkettypes.DefaultGenesisState()
	feemarketGenesis.Params.MinGasMultiplier = sdk.OneDec()
	genesis[feemarkettypes.ModuleName] = enc.Codec.MustMarshalJSON(feemarketGenesis)
	genesis[authtypes.ModuleName] = enc.Codec.MustMarshalJSON(authtypes.NewGenesisState(authtypes.DefaultParams(), accounts))
	genesis[banktypes.ModuleName] = enc.Codec.MustMarshalJSON(banktypes.NewGenesisState(banktypes.DefaultGenesisState().Params, balances, supply, nil, nil))
	genesis[stakingtypes.ModuleName] = enc.Codec.MustMarshalJSON(stakingtypes.NewGenesisState(stakingtypes.DefaultParams(), []stakingtypes.Validator{{
		OperatorAddress: sdk.ValAddress(valConsAddr).String(), ConsensusPubkey: pkAny, Status: stakingtypes.Bonded,
		Tokens: bond, DelegatorShares: sdk.OneDec(), UnbondingTime: time.Unix(0, 0).UTC(),
		Commission: stakingtypes.NewCommission(sdk.ZeroDec(), sdk.ZeroDec(), sdk.ZeroDec()), MinSelfDelegation: sdk.ZeroInt(),
	}}, []stakingtypes.Delegation{stakingtypes.NewDelegation(accounts[0].GetAddress(), sdk.ValAddress(valConsAddr), sdk.OneDec())}))
	state, err := json.Marshal(genesis)
	require.NoError(t, err)
	params := *DefaultConsensusParams
	params.Block = &tmproto.BlockParams{MaxBytes: DefaultConsensusParams.Block.MaxBytes, MaxGas: maxGas}
	a.InitChain(abci.RequestInitChain{ChainId: TestAppChainID, ConsensusParams: &params, AppStateBytes: state})
	a.Commit()
	parentHash := a.LastCommitID().Hash

	code := common.FromHex("0x6000600060006000600073" + "0000000000000000000000000000000000000004" + "5af15060006000a000")
	// Above the fee market's initial base fee.
	gasPrice := big.NewInt(10_000_000_000)
	var txs [][]byte
	for _, key := range keys {
		msg := evmtypes.NewTx(ethChainID, 0, nil, big.NewInt(0), 300000, gasPrice, nil, nil, code, nil)
		msg.From = key.PubKey().Address().Bytes()
		require.NoError(t, msg.Sign(ethtypes.LatestSignerForChainID(ethChainID), tests.NewSigner(key)))
		option, err := codectypes.NewAnyWithValue(&evmtypes.ExtensionOptionsEthereumTx{})
		require.NoError(t, err)
		builder := enc.TxConfig.NewTxBuilder().(authtx.ExtensionOptionsTxBuilder)
		builder.SetExtensionOptions(option)
		require.NoError(t, builder.SetMsgs(msg))
		builder.SetFeeAmount(sdk.NewCoins(sdk.NewCoin(denom, sdk.NewIntFromBigInt(new(big.Int).Mul(gasPrice, big.NewInt(300000))))))
		builder.SetGasLimit(300000)
		raw, err := enc.TxConfig.TxEncoder()(builder.GetTx())
		require.NoError(t, err)
		txs = append(txs, raw)
	}
	begin := abci.RequestBeginBlock{Header: tmproto.Header{
		ChainID: TestAppChainID, Height: 2, Time: time.Unix(2, 0).UTC(), ProposerAddress: valConsAddr,
	}}
	a.BeginBlock(begin)
	var want []abci.ResponseDeliverTx
	for _, tx := range txs {
		want = append(want, a.DeliverTx(abci.RequestDeliverTx{Tx: tx}))
	}
	a.EndBlock(abci.RequestEndBlock{Height: 2})
	wantHash := a.Commit().Data
	return replayFixture{db, replay.Request{ChainID: TestAppChainID, ParentHash: parentHash, BeginBlock: &begin, Txs: txs}, want, wantHash}
}

// A long-lived replay process builds a fresh app per request over a shared
// read-only database while other requests execute on it.
func TestReplayOnDBMatchesExecution(t *testing.T) {
	f := newReplayFixture(t, DefaultConsensusParams.Block.MaxGas)
	db, req, want, wantHash := f.db, f.req, f.want, f.wantHash
	for _, r := range want {
		require.Zero(t, r.Code, r.Log)
	}
	before := dbDigest(t, db)

	run := func() *replay.Result {
		res, err := ReplayOnDB(db, t.TempDir(), req, func(string) {})
		require.NoError(t, err)
		return res
	}
	first := run()
	require.Equal(t, wantHash, first.AppHash)
	require.Len(t, first.Transactions, len(req.Txs))
	for i, r := range first.Responses.DeliverTxs {
		require.Equal(t, want[i].Data, r.Data)
		require.Equal(t, want[i].GasUsed, r.GasUsed)
	}
	for i, obs := range first.Transactions {
		require.Equal(t, i, obs.CosmosIndex)
		require.Empty(t, obs.Error)
		require.False(t, obs.Failed)
		require.Contains(t, string(obs.Trace), `"to_addr":"0x0000000000000000000000000000000000000004"`)
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := run()
			require.NoError(t, replay.Verify(first.Responses, got.Responses))
			require.Equal(t, first.AppHash, got.AppHash)
			require.Equal(t, first.Transactions, got.Transactions)
		}()
	}
	wg.Wait()
	require.Equal(t, before, dbDigest(t, db), "replay must not write the shared database")

	bad := req
	bad.ParentHash = make([]byte, 32)
	_, err := ReplayOnDB(db, t.TempDir(), bad, func(string) {})
	require.ErrorContains(t, err, "parent commit")
}

// The SDK charges block gas before writing a transaction's messages: the
// second deployment exceeds the block gas limit and its state is dropped, so
// its observation is failed and its logs become error events.
func TestReplayBlockGasExceeded(t *testing.T) {
	maxGas := newReplayFixture(t, DefaultConsensusParams.Block.MaxGas).want[0].GasUsed * 3 / 2
	f := newReplayFixture(t, maxGas)
	require.Zero(t, f.want[0].Code)
	require.True(t, replay.ExceedsBlockGas(&f.want[1]), f.want[1].Log)
	require.Equal(t, uint32(11), f.want[2].Code)

	res, err := ReplayOnDB(f.db, t.TempDir(), f.req, func(string) {})
	require.NoError(t, err)
	require.Equal(t, f.wantHash, res.AppHash)
	require.Len(t, res.Transactions, 2, "the third transaction never ran")

	var ok, exceeded dtypes.TraceResult
	require.NoError(t, json.Unmarshal(res.Transactions[0].Trace, &ok))
	require.NoError(t, json.Unmarshal(res.Transactions[1].Trace, &exceeded))
	require.False(t, res.Transactions[0].Failed)
	require.Len(t, ok.Events, 1)
	require.True(t, res.Transactions[1].Failed)
	require.Empty(t, exceeded.Traces)
	require.Empty(t, exceeded.Events)
	require.Len(t, exceeded.ErrorEvents, 1)
	require.Empty(t, exceeded.StorageContracts)
	for _, tr := range exceeded.ErrorTraces {
		if len(tr.TraceAddress) == 0 {
			require.Equal(t, replay.BlockGasExceeded, tr.Error)
		}
	}
}
