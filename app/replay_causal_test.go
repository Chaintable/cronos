package app

import (
	"encoding/json"
	"math/big"
	"math/rand"
	"strings"
	"testing"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	tmproto "github.com/cometbft/cometbft/proto/tendermint/types"
	tmtypes "github.com/cometbft/cometbft/types"
	dbm "github.com/cosmos/cosmos-db"
	ct "github.com/crypto-org-chain/cronos/x/cronos/types"
	"github.com/ethereum/go-ethereum/common"
	eth "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/evmos/ethermint/crypto/ethsecp256k1"
	"github.com/evmos/ethermint/debank/replay"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/evmos/ethermint/tests"
	evm "github.com/evmos/ethermint/x/evm/types"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/log"
	"cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/testutil/mock"
	sims "github.com/cosmos/cosmos-sdk/testutil/sims"
	sdk "github.com/cosmos/cosmos-sdk/types"
	auth "github.com/cosmos/cosmos-sdk/x/auth/types"
	bank "github.com/cosmos/cosmos-sdk/x/bank/types"
)

// The donor converts a voucher into CRC21 and funds the spender with native
// coins in one signed Cosmos tx. Two other signers let the omission control
// distinguish an ante fee failure from a contract-level transferFrom revert.
func TestReplayCosmosCreditBeforeEVM(t *testing.T) {
	db := dbm.NewMemDB()
	opts := MinimalOptionsMap{flags.FlagHome: t.TempDir(), FlagDisableOptimisticExecution: true}
	open := func(latest bool) *App {
		return New(log.NewNopLogger(), db, nil, latest, opts, baseapp.SetChainID(TestAppChainID), baseapp.SetIAVLDisableFastNode(true))
	}
	a := open(true)
	denom := evm.DefaultEVMDenom
	voucher := "ibc/" + strings.Repeat("A", 64)
	var keys []*ethsecp256k1.PrivKey
	var addresses []common.Address
	var accounts []auth.GenesisAccount
	var balances []bank.Balance
	for i := 0; i < 3; i++ {
		key, err := ethsecp256k1.GenerateKey()
		require.NoError(t, err)
		addr := sdk.AccAddress(key.PubKey().Address())
		keys = append(keys, key)
		addresses = append(addresses, common.BytesToAddress(addr))
		accounts = append(accounts, auth.NewBaseAccount(addr, key.PubKey(), uint64(i), 0))
		coins := sdk.NewCoins(sdk.NewCoin(denom, math.NewIntWithDecimal(10, 18)))
		if i == 0 {
			coins = coins.Add(sdk.NewInt64Coin(voucher, 100))
		}
		if i == 1 {
			coins = sdk.NewCoins(sdk.NewInt64Coin(denom, 1))
		}
		balances = append(balances, bank.Balance{Address: addr.String(), Coins: coins})
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
	ctx := a.GetContextForFinalizeBlock(nil).WithBlockHeader(tmproto.Header{ChainID: TestAppChainID, Height: 1, ProposerAddress: val.Address})
	token, err := a.CronosKeeper.DeployModuleCRC21(ctx, voucher)
	require.NoError(t, err)
	a.CronosKeeper.SetAutoContractForDenom(ctx, voucher, token)
	finalize := func(h int64, txs ...[]byte) *abci.RequestFinalizeBlock {
		return &abci.RequestFinalizeBlock{Height: h, Time: time.Unix(h, 0).UTC(), ProposerAddress: val.Address, Txs: txs}
	}
	_, err = a.FinalizeBlock(finalize(1))
	require.NoError(t, err)
	_, err = a.Commit()
	require.NoError(t, err)
	signEVM := func(who int, nonce uint64, to *common.Address, value int64, data []byte) []byte {
		tx := evm.NewTx(TestEthChainID, nonce, to, big.NewInt(value), 300000, nil, big.NewInt(100000000000), big.NewInt(0), data, nil)
		tx.From = addresses[who].Bytes()
		require.NoError(t, tx.Sign(eth.LatestSignerForChainID(TestEthChainID), tests.NewSigner(keys[who])))
		signed, err := tx.BuildTx(a.TxConfig().NewTxBuilder(), denom)
		require.NoError(t, err)
		raw, err := a.TxConfig().TxEncoder()(signed)
		require.NoError(t, err)
		return raw
	}
	call := func(name string, args ...any) []byte {
		data, err := ct.ModuleCRC21Contract.ABI.Pack(name, args...)
		require.NoError(t, err)
		return data
	}
	approve := signEVM(0, 0, &token, 0, call("approve", addresses[2], big.NewInt(100)))
	res, err := a.FinalizeBlock(finalize(2, approve))
	require.NoError(t, err)
	require.Zero(t, res.TxResults[0].Code, res.TxResults[0].Log)
	_, err = a.Commit()
	require.NoError(t, err)
	donor := sdk.AccAddress(addresses[0].Bytes())
	spender := sdk.AccAddress(addresses[1].Bytes())
	account := a.AccountKeeper.GetAccount(a.GetContextForCheckTx(nil), donor)
	cosmos, err := sims.GenSignedMockTx(rand.New(rand.NewSource(1)), a.TxConfig(), []sdk.Msg{
		&bank.MsgSend{FromAddress: donor.String(), ToAddress: spender.String(), Amount: sdk.NewCoins(sdk.NewCoin(denom, math.NewIntWithDecimal(1, 18)))},
		&ct.MsgConvertVouchers{Address: donor.String(), Coins: sdk.NewCoins(sdk.NewInt64Coin(voucher, 100))},
	}, sdk.NewCoins(sdk.NewCoin(denom, math.NewIntWithDecimal(1, 17))), 1000000, TestAppChainID, []uint64{account.GetAccountNumber()}, []uint64{account.GetSequence()}, keys[0])
	require.NoError(t, err)
	credit, err := a.TxConfig().TxEncoder()(cosmos)
	require.NoError(t, err)
	spend := signEVM(1, 0, &addresses[0], 1, nil)
	mapped := signEVM(2, 0, &token, 0, call("transferFrom", addresses[0], addresses[1], big.NewInt(100)))
	// Deploy a child that emits LOG0 and reverts. The next constructor catches
	// that failure and emits a committed LOG0. The final constructor calls the
	// identity precompile successfully, then emits LOG0 and reverts itself.
	childCode := common.FromHex("0x60006000a060006000fd")
	initCode := append(common.FromHex("0x600a600c600039600a6000f3"), childCode...)
	child := crypto.CreateAddress(addresses[2], 1)
	constructor := func(target common.Address, suffix string) []byte {
		code := common.FromHex("0x6000600060006000600073")
		code = append(code, target.Bytes()...)
		return append(code, common.FromHex("0x5af15060006000a0"+suffix)...)
	}
	deploy := signEVM(2, 1, nil, 0, initCode)
	caught := signEVM(2, 2, nil, 0, constructor(child, "00"))
	reverted := signEVM(2, 3, nil, 0, constructor(common.BytesToAddress([]byte{4}), "60006000fd"))
	// Keep two signed messages inside the same Cosmos envelope.
	var msgs []sdk.Msg
	var fees sdk.Coins
	var gas uint64
	builder := a.TxConfig().NewTxBuilder()
	for i, raw := range [][]byte{deploy, caught} {
		tx, err := a.TxConfig().TxDecoder()(raw)
		require.NoError(t, err)
		if i == 0 {
			builder, err = a.TxConfig().WrapTxBuilder(tx)
			require.NoError(t, err)
		}
		msgs = append(msgs, tx.GetMsgs()...)
		feeTx := tx.(sdk.FeeTx)
		fees = fees.Add(feeTx.GetFee()...)
		gas += feeTx.GetGas()
	}
	require.NoError(t, builder.SetMsgs(msgs...))
	builder.SetFeeAmount(fees)
	builder.SetGasLimit(gas)
	bundle, err := a.TxConfig().TxEncoder()(builder.GetTx())
	require.NoError(t, err)
	full := finalize(3, credit, spend, mapped, bundle, reverted)
	normal, err := a.FinalizeBlock(full)
	require.NoError(t, err)
	for _, r := range normal.TxResults {
		require.Zero(t, r.Code, r.Log)
	}
	checkEVM := func(r *abci.ExecTxResult, wantFailure bool) {
		resp, err := evm.DecodeTxResponse(r.Data)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Hash)
		require.Equal(t, wantFailure, resp.VmError != "", resp.VmError)
	}
	checkEVM(normal.TxResults[1], false)
	checkEVM(normal.TxResults[2], false)
	run := func(req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, []replay.Transaction) {
		copy := open(false)
		require.NoError(t, copy.LoadHeight(2))
		observed := ObserveReplayTransactions(copy.EvmKeeper)
		out, err := copy.FinalizeBlock(req)
		require.NoError(t, err)
		return out, *observed
	}
	replayed, observed := run(full)
	require.Equal(t, normal, replayed)
	require.Len(t, observed, 5)
	for i, o := range observed {
		require.Empty(t, o.Error, "observation %d", i)
	}
	require.Equal(t, 3, observed[2].CosmosIndex)
	require.Equal(t, 0, observed[2].MessageIndex)
	require.Equal(t, 3, observed[3].CosmosIndex)
	require.Equal(t, 1, observed[3].MessageIndex)
	var caughtTrace, revertedTrace dtypes.TraceResult
	require.NoError(t, json.Unmarshal(observed[3].Trace, &caughtTrace))
	require.NoError(t, json.Unmarshal(observed[4].Trace, &revertedTrace))
	require.Len(t, caughtTrace.Traces, 1)
	require.Len(t, caughtTrace.ErrorTraces, 1)
	require.Len(t, caughtTrace.Events, 1)
	require.Len(t, caughtTrace.ErrorEvents, 1)
	require.Equal(t, caughtTrace.Traces[0].ID, caughtTrace.ErrorTraces[0].ParentTraceID)
	require.Equal(t, caughtTrace.ErrorTraces[0].ID, caughtTrace.ErrorEvents[0].ParentTraceID)
	require.Empty(t, revertedTrace.Traces)
	require.Len(t, revertedTrace.ErrorTraces, 2)
	require.Empty(t, revertedTrace.Events)
	require.Len(t, revertedTrace.ErrorEvents, 1)
	omitted, _ := run(finalize(3, spend, mapped))
	require.NotZero(t, omitted.TxResults[0].Code)
	require.Contains(t, omitted.TxResults[0].Log, "insufficient")
	require.Zero(t, omitted.TxResults[1].Code)
	checkEVM(omitted.TxResults[1], true)
	require.NotEqual(t, normal.AppHash, omitted.AppHash)
}
