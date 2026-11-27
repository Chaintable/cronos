package app

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"cosmossdk.io/log"
	sdkmath "cosmossdk.io/math"
	abci "github.com/cometbft/cometbft/abci/types"
	tmtypes "github.com/cometbft/cometbft/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/testutil/mock"
	sims "github.com/cosmos/cosmos-sdk/testutil/sims"
	sdk "github.com/cosmos/cosmos-sdk/types"
	auth "github.com/cosmos/cosmos-sdk/x/auth/types"
	bank "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/ethereum/go-ethereum/common"
	eth "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/evmos/ethermint/debank/tracer"
	dt "github.com/evmos/ethermint/debank/types"
	evm "github.com/evmos/ethermint/x/evm/types"
	fee "github.com/evmos/ethermint/x/feemarket/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// Synthetic blocks on a private DB, with a mainnet chain ID and v1.5 Prague/fee parameters.
func TestReplayExecutionFeesAndDelegation(t *testing.T) {
	const chain = "cronosmainnet_25-1"
	id := big.NewInt(25)
	db := dbm.NewMemDB()
	opts := MinimalOptionsMap{flags.FlagHome: t.TempDir(), FlagDisableOptimisticExecution: true}
	open := func(latest bool) *App {
		return New(log.NewNopLogger(), db, nil, latest, opts, baseapp.SetChainID(chain), baseapp.SetIAVLDisableFastNode(true))
	}
	a := open(true)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyAuth, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender, authority := crypto.PubkeyToAddress(key.PublicKey), crypto.PubkeyToAddress(keyAuth.PublicKey)
	signer := eth.LatestSignerForChainID(id)
	denom := evm.DefaultEVMDenom
	addr := sdk.AccAddress(sender.Bytes())
	genesisBalance := sdkmath.NewIntWithDecimal(50, 18)
	accts := []auth.GenesisAccount{auth.NewBaseAccountWithAddress(addr)}
	balances := []bank.Balance{{Address: addr.String(), Coins: sdk.NewCoins(sdk.NewCoin(denom, genesisBalance))}}
	pub, err := mock.NewPV().GetPubKey()
	require.NoError(t, err)
	val := tmtypes.NewValidator(pub, 1)
	g, err := sims.GenesisStateWithValSet(a.AppCodec(), a.DefaultGenesis(), tmtypes.NewValidatorSet([]*tmtypes.Validator{val}), accts, balances...)
	require.NoError(t, err)
	var fg fee.GenesisState
	a.AppCodec().MustUnmarshalJSON(g[fee.ModuleName], &fg)
	fg.Params.MinGasMultiplier = sdkmath.LegacyNewDecWithPrec(5, 1)
	fg.Params.MinGasPrice = sdkmath.LegacyNewDec(375000000000)
	fg.Params.BaseFee = sdkmath.NewInt(375000000000)
	g[fee.ModuleName] = a.AppCodec().MustMarshalJSON(&fg)
	bz, err := json.Marshal(g)
	require.NoError(t, err)
	_, err = a.InitChain(&abci.RequestInitChain{ChainId: chain, AppStateBytes: bz, ConsensusParams: DefaultConsensusParams})
	require.NoError(t, err)
	req := func(h int64, txs ...[]byte) *abci.RequestFinalizeBlock {
		return &abci.RequestFinalizeBlock{Height: h, Time: time.Unix(h, 0).UTC(), ProposerAddress: val.Address, Txs: txs}
	}
	_, err = a.FinalizeBlock(req(1))
	require.NoError(t, err)
	_, err = a.Commit()
	require.NoError(t, err)
	runtimeCode := common.FromHex("0x36156008575f5ffd5b5f5fa000") // no calldata: emit LOG0; with calldata: REVERT
	initCode := append(common.FromHex("0x600d600c600039600d6000f3"), runtimeCode...)
	target := crypto.CreateAddress(sender, 0)
	authorization, err := eth.SignSetCode(keyAuth, eth.SetCodeAuthorization{ChainID: *uint256.NewInt(25), Address: target, Nonce: 0})
	require.NoError(t, err)
	const base = 375000000000
	const tip = 50
	const gasLimit = 300000
	txs := []eth.TxData{
		&eth.DynamicFeeTx{ChainID: id, Nonce: 0, Gas: gasLimit, GasFeeCap: big.NewInt(base + 10), GasTipCap: big.NewInt(tip), Data: initCode},
		&eth.SetCodeTx{ChainID: uint256.NewInt(25), Nonce: 1, Gas: gasLimit, GasFeeCap: uint256.NewInt(base + 100), GasTipCap: uint256.NewInt(tip), To: authority, AuthList: []eth.SetCodeAuthorization{authorization}},
		&eth.DynamicFeeTx{ChainID: id, Nonce: 2, Gas: gasLimit, GasFeeCap: big.NewInt(base + 10), GasTipCap: big.NewInt(tip), To: &authority, Data: []byte{1}},
		&eth.DynamicFeeTx{ChainID: id, Nonce: 3, Gas: 30000, GasFeeCap: big.NewInt(base + 10), GasTipCap: big.NewInt(tip), To: &authority},
	}
	for i, data := range txs {
		tx, err := eth.SignNewTx(key, signer, data)
		require.NoError(t, err)
		m := new(evm.MsgEthereumTx)
		require.NoError(t, m.FromSignedEthereumTx(tx, signer))
		cos, err := m.BuildTx(a.TxConfig().NewTxBuilder(), denom)
		require.NoError(t, err)
		raw, err := a.TxConfig().TxEncoder()(cos)
		require.NoError(t, err)
		height := int64(i + 2)
		b := req(height, raw)
		parentBalance := a.BankKeeper.GetBalance(a.GetContextForCheckTx(nil), addr, denom).Amount
		mirror := open(false)
		require.NoError(t, mirror.LoadHeight(height-1))
		observed := ObserveReplayTransactions(mirror.EvmKeeper)
		expected, err := a.FinalizeBlock(b)
		require.NoError(t, err)
		got, err := mirror.FinalizeBlock(b)
		require.NoError(t, err)
		require.Equal(t, expected, got, "full ABCI response, events, AppHash and receipts")
		require.Zero(t, got.TxResults[0].Code, got.TxResults[0].Log)
		receipt, err := evm.DecodeTxResponse(got.TxResults[0].Data)
		require.NoError(t, err)
		require.Len(t, *observed, 1)
		o := (*observed)[0]
		require.Empty(t, o.Error)
		require.Equal(t, tx.Hash(), o.Hash)
		require.Equal(t, receipt.Hash, o.Hash.Hex())
		require.Equal(t, i == 2, receipt.Failed())
		require.Equal(t, i == 2, o.Failed)
		if i < 3 {
			require.EqualValues(t, gasLimit/2, receipt.GasUsed)
		}
		require.Equal(t, receipt.GasUsed, o.GasUsed)
		wantPrice := int64(base + 10)
		if i == 1 {
			wantPrice = base + tip
		}
		bf := tracer.BuildPipelineTransaction(tx, 0, sender, new(big.Int).SetUint64(receipt.GasUsed), big.NewInt(base), !receipt.Failed())
		require.EqualValues(t, wantPrice, bf.GasPrice.Int64())
		require.Equal(t, tx.GasFeeCap(), bf.GasFeeCap)
		require.Equal(t, tx.GasTipCap(), bf.GasTipCap)
		require.Equal(t, receipt.GasUsed, bf.GasUsed.Uint64())
		require.Equal(t, !receipt.Failed(), bf.Status)
		var trace dt.TraceResult
		require.NoError(t, json.Unmarshal(o.Trace, &trace))
		frames := trace.Traces
		if i == 2 {
			frames = trace.ErrorTraces
		}
		require.Len(t, frames, 1)
		require.Less(t, frames[0].GasUsed.Uint64(), receipt.GasUsed)
		if i == 3 {
			require.Greater(t, receipt.GasUsed, tx.Gas()/2)
			require.Equal(t, uint64(21000)+frames[0].GasUsed.Uint64(), receipt.GasUsed)
		}
		_, err = a.Commit()
		require.NoError(t, err)
		ctx := a.GetContextForCheckTx(nil)
		actualBase := a.FeeMarketKeeper.GetBaseFee(ctx)
		require.EqualValues(t, base, actualBase.Int64())
		post := a.BankKeeper.GetBalance(ctx, addr, denom).Amount
		net := parentBalance.Sub(post).BigInt()
		require.Equal(t, new(big.Int).Mul(new(big.Int).SetUint64(receipt.GasUsed), big.NewInt(wantPrice)), net)
		escrow := new(big.Int).Mul(new(big.Int).SetUint64(tx.Gas()), big.NewInt(wantPrice))
		refund := new(big.Int).Sub(escrow, net)
		require.Equal(t, new(big.Int).Mul(new(big.Int).SetUint64(tx.Gas()-receipt.GasUsed), big.NewInt(wantPrice)), refund)
		collector := auth.NewModuleAddress(auth.FeeCollectorName).String()
		var prepaymentEvents, refundEvents []string
		for _, event := range got.TxResults[0].Events {
			if event.Type != "transfer" {
				continue
			}
			attrs := map[string]string{}
			for _, attr := range event.Attributes {
				attrs[attr.Key] = attr.Value
			}
			if attrs["sender"] == addr.String() && attrs["recipient"] == collector {
				prepaymentEvents = append(prepaymentEvents, attrs["amount"])
			}
			if attrs["sender"] == collector && attrs["recipient"] == addr.String() {
				refundEvents = append(refundEvents, attrs["amount"])
			}
		}
		require.Equal(t, []string{escrow.String() + denom}, prepaymentEvents)
		require.Equal(t, []string{refund.String() + denom}, refundEvents)
		t.Logf("type=%d create=%v status=%t signed_gas=%d charged=%d root_vm_gas=%d effective_price=%d sender_net_debit=%s escrow_minus_debit=%s", tx.Type(), tx.To() == nil, bf.Status, tx.Gas(), receipt.GasUsed, frames[0].GasUsed, wantPrice, net, refund)
		if i == 0 {
			require.Equal(t, strings.ToLower(target.Hex()), bf.To)
			require.Equal(t, bf.To, frames[0].To)
			codeHash := a.EvmKeeper.GetAccount(ctx, target).CodeHash
			require.Equal(t, runtimeCode, a.EvmKeeper.GetCode(ctx, common.BytesToHash(codeHash)))
		} else {
			require.Equal(t, strings.ToLower(authority.Hex()), bf.To)
			require.EqualValues(t, 1, a.EvmKeeper.GetNonce(ctx, authority))
			codeHash := a.EvmKeeper.GetAccount(ctx, authority).CodeHash
			require.Equal(t, eth.AddressToDelegation(target), a.EvmKeeper.GetCode(ctx, common.BytesToHash(codeHash)))
			if i == 1 || i == 3 {
				require.Len(t, receipt.Logs, 1)
				require.Equal(t, authority, common.HexToAddress(receipt.Logs[0].Address))
				require.Len(t, trace.Events, 1)
				require.Equal(t, bf.To, trace.Events[0].Address)
			} else {
				require.Empty(t, receipt.Logs)
			}
		}
	}
}
