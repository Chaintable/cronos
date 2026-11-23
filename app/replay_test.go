package app

import (
	"crypto/sha256"
	"encoding/json"
	"math/big"
	"sync"
	"testing"
	"time"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/libs/log"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	dbm "github.com/tendermint/tm-db"
	"github.com/tharsis/ethermint/crypto/ethsecp256k1"
	"github.com/tharsis/ethermint/debank/replay"
	"github.com/tharsis/ethermint/tests"
	evmtypes "github.com/tharsis/ethermint/x/evm/types"
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

// A long-lived replay process builds a fresh app per request over a shared
// read-only database while other requests execute on it.
func TestReplayOnDBMatchesExecution(t *testing.T) {
	db := dbm.NewMemDB()
	enc := MakeEncodingConfig()
	a := New(log.NewNopLogger(), db, nil, true, map[int64]bool{}, t.TempDir(), 0, enc, EmptyAppOptions{})
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
		coins := sdk.NewCoins(sdk.NewCoin(denom, sdk.NewIntWithDecimal(10, 18)))
		balances = append(balances, banktypes.Balance{Address: addr.String(), Coins: coins})
		supply = supply.Add(coins...)
	}
	bond := sdk.NewInt(1000000)
	balances = append(balances, banktypes.Balance{Address: authtypes.NewModuleAddress(stakingtypes.BondedPoolName).String(), Coins: sdk.NewCoins(sdk.NewCoin(sdk.DefaultBondDenom, bond))})
	supply = supply.Add(sdk.NewCoin(sdk.DefaultBondDenom, bond))

	genesis := NewDefaultGenesisState(enc.Marshaler)
	genesis[authtypes.ModuleName] = enc.Marshaler.MustMarshalJSON(authtypes.NewGenesisState(authtypes.DefaultParams(), accounts))
	genesis[banktypes.ModuleName] = enc.Marshaler.MustMarshalJSON(banktypes.NewGenesisState(banktypes.DefaultGenesisState().Params, balances, supply, nil))
	genesis[stakingtypes.ModuleName] = enc.Marshaler.MustMarshalJSON(stakingtypes.NewGenesisState(stakingtypes.DefaultParams(), []stakingtypes.Validator{{
		OperatorAddress: sdk.ValAddress(valConsAddr).String(), ConsensusPubkey: pkAny, Status: stakingtypes.Bonded,
		Tokens: bond, DelegatorShares: sdk.OneDec(), UnbondingTime: time.Unix(0, 0).UTC(),
		Commission: stakingtypes.NewCommission(sdk.ZeroDec(), sdk.ZeroDec(), sdk.ZeroDec()), MinSelfDelegation: sdk.ZeroInt(),
	}}, []stakingtypes.Delegation{stakingtypes.NewDelegation(accounts[0].GetAddress(), sdk.ValAddress(valConsAddr), sdk.OneDec())}))
	state, err := json.Marshal(genesis)
	require.NoError(t, err)
	a.InitChain(abci.RequestInitChain{ChainId: TestAppChainID, ConsensusParams: DefaultConsensusParams, AppStateBytes: state})
	a.Commit()
	parentHash := a.LastCommitID().Hash

	// Each sender deploys a contract that calls the identity precompile and emits LOG0.
	code := common.FromHex("0x6000600060006000600073" + "0000000000000000000000000000000000000004" + "5af15060006000a000")
	var txs [][]byte
	for _, key := range keys {
		msg := evmtypes.NewTx(ethChainID, 0, nil, big.NewInt(0), 300000, big.NewInt(1), code, nil)
		msg.From = common.BytesToAddress(key.PubKey().Address()).Hex()
		require.NoError(t, msg.Sign(ethtypes.LatestSignerForChainID(ethChainID), tests.NewSigner(key)))
		option, err := codectypes.NewAnyWithValue(&evmtypes.ExtensionOptionsEthereumTx{})
		require.NoError(t, err)
		builder := enc.TxConfig.NewTxBuilder().(authtx.ExtensionOptionsTxBuilder)
		builder.SetExtensionOptions(option)
		require.NoError(t, builder.SetMsgs(msg))
		builder.SetFeeAmount(sdk.NewCoins(sdk.NewCoin(denom, sdk.NewInt(300000))))
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
		r := a.DeliverTx(abci.RequestDeliverTx{Tx: tx})
		require.Zero(t, r.Code, r.Log)
		want = append(want, r)
	}
	a.EndBlock(abci.RequestEndBlock{Height: 2})
	wantHash := a.Commit().Data
	before := dbDigest(t, db)

	req := replay.Request{ChainID: TestAppChainID, ParentHash: parentHash, BeginBlock: &begin, Txs: txs}
	run := func() *replay.Result {
		res, err := ReplayOnDB(db, t.TempDir(), req, func(string) {})
		require.NoError(t, err)
		return res
	}
	first := run()
	require.Equal(t, wantHash, first.AppHash)
	require.Len(t, first.Transactions, len(keys))
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
	_, err = ReplayOnDB(db, t.TempDir(), bad, func(string) {})
	require.ErrorContains(t, err, "parent commit")
}
