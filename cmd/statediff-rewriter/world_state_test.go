package main

import (
	"slices"
	"testing"

	"cosmossdk.io/collections"
	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	dtypes "github.com/evmos/ethermint/debank/types"
	etherminttypes "github.com/evmos/ethermint/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestWorldStateDecoderAndProjection(t *testing.T) {
	decoder, err := newWorldStateDecoder("basecro")
	require.NoError(t, err)
	address := common.HexToAddress("0x0000000000000000000000000000000000001234")
	code := []byte{0x60, 0x00}
	codeHash := crypto.Keccak256Hash(code)
	account := &etherminttypes.EthAccount{
		BaseAccount: &authtypes.BaseAccount{Address: sdk.AccAddress(address.Bytes()).String(), Sequence: 7},
		CodeHash:    codeHash.Hex(),
	}
	accountValue, err := decoder.accountValues.Encode(account)
	require.NoError(t, err)
	accountChanges := &iavl.ChangeSet{Pairs: []*iavl.KVPair{{
		Key: append([]byte{authAccountPrefix}, address.Bytes()...), Value: accountValue,
	}}}
	accounts, err := decoder.decodeAccounts(accountChanges)
	require.NoError(t, err)
	require.Equal(t, []accountMutation{{
		Address: address, State: authState{Nonce: 7, CodeHash: codeHash},
	}}, accounts)

	balanceKey := collections.Join(sdk.AccAddress(address.Bytes()), "basecro")
	encodedBalanceKey := make([]byte, 1+decoder.balanceKeys.Size(balanceKey))
	encodedBalanceKey[0] = bankBalancePrefix
	written, err := decoder.balanceKeys.Encode(encodedBalanceKey[1:], balanceKey)
	require.NoError(t, err)
	require.Equal(t, len(encodedBalanceKey)-1, written)
	balanceValue, err := banktypes.BalanceValueCodec.Encode(math.NewInt(99))
	require.NoError(t, err)
	balances, err := decoder.decodeBalances(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{
		Key: encodedBalanceKey, Value: balanceValue,
	}}})
	require.NoError(t, err)
	var balance [32]byte
	balance[31] = 99
	require.Equal(t, []balanceMutation{{Address: address, Balance: balance}}, balances)

	codes, err := decodeCodes(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{
		Key: append([]byte{evmCodePrefix}, codeHash.Bytes()...), Value: code,
	}}})
	require.NoError(t, err)
	require.Equal(t, []codeMutation{{CodeHash: codeHash, Code: code}}, codes)

	projection := newWorldStateProjection()
	expected := projection.apply(worldStateDelta{
		height: 2, accounts: accounts, balances: balances, codes: codes,
	})
	require.Len(t, expected.newAccounts, 1)
	require.Equal(t, crypto.Keccak256Hash(address.Bytes()), expected.newAccounts[0].Address)
	require.Equal(t, uint64(7), expected.newAccounts[0].Nonce)
	require.Equal(t, codeHash, expected.newAccounts[0].CodeHash)
	require.Equal(t, uint64(99), expected.newAccounts[0].Balance.Uint64())
	require.Empty(t, expected.deletedAccounts)
	require.Equal(t, codeHash, expected.codeWrites[0].CodeHash)

	expected = projection.apply(worldStateDelta{
		height:   3,
		accounts: []accountMutation{{Address: address, Delete: true}},
		balances: []balanceMutation{{Address: address, Delete: true}},
		codes:    []codeMutation{{CodeHash: codeHash, Delete: true}},
	})
	require.Empty(t, expected.newAccounts)
	require.Equal(t, []common.Hash{crypto.Keccak256Hash(address.Bytes())}, expected.deletedAccounts)
	require.Equal(t, []common.Hash{codeHash}, expected.codeDeletes)
}

func TestWorldStateProjectionIgnoresRawRewriteWithoutEVMChange(t *testing.T) {
	address := common.HexToAddress("0x0000000000000000000000000000000000001234")
	projection := newWorldStateProjection()
	projection.accounts[address] = authState{Nonce: 3, CodeHash: emptyEVMCodeHash}
	expected := projection.apply(worldStateDelta{
		height:   2,
		accounts: []accountMutation{{Address: address, State: authState{Nonce: 3, CodeHash: emptyEVMCodeHash}}},
	})
	require.Empty(t, expected.newAccounts)
	require.Empty(t, expected.deletedAccounts)
}

func TestWorldStateProjectionIgnoresEmptyAccountPresenceOnlyChanges(t *testing.T) {
	address := common.HexToAddress("0x0000000000000000000000000000000000001234")
	projection := newWorldStateProjection()
	expected := projection.apply(worldStateDelta{
		height: 2,
		accounts: []accountMutation{{
			Address: address,
			State:   authState{CodeHash: emptyEVMCodeHash},
		}},
	})
	require.Empty(t, expected.newAccounts)
	require.Empty(t, expected.deletedAccounts)

	expected = projection.apply(worldStateDelta{
		height:   3,
		accounts: []accountMutation{{Address: address, Delete: true}},
	})
	require.Empty(t, expected.newAccounts)
	require.Empty(t, expected.deletedAccounts)
}

func TestWorldStateDecoderSupportsModuleAccounts(t *testing.T) {
	decoder, err := newWorldStateDecoder("basecro")
	require.NoError(t, err)
	account := authtypes.NewEmptyModuleAccount("world-state-audit-test")
	accountValue, err := decoder.accountValues.Encode(account)
	require.NoError(t, err)
	address := common.BytesToAddress(account.GetAddress())
	accounts, err := decoder.decodeAccounts(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{
		Key: append([]byte{authAccountPrefix}, account.GetAddress()...), Value: accountValue,
	}}})
	require.NoError(t, err)
	require.Equal(t, []accountMutation{{
		Address: address, State: authState{CodeHash: emptyEVMCodeHash},
	}}, accounts)
}

func TestDecodeCodesRejectsHashMismatch(t *testing.T) {
	_, err := decodeCodes(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{
		Key: append([]byte{evmCodePrefix}, common.HexToHash("0x01").Bytes()...), Value: []byte{0x60},
	}}})
	require.ErrorContains(t, err, "body hash")
}

func TestDecodeBalancesFiltersOtherDenomsAndNonEVMAddresses(t *testing.T) {
	decoder, err := newWorldStateDecoder("basecro")
	require.NoError(t, err)
	value, err := banktypes.BalanceValueCodec.Encode(math.NewInt(1))
	require.NoError(t, err)
	makePair := func(address sdk.AccAddress, denom string) *iavl.KVPair {
		key := collections.Join(address, denom)
		encoded := make([]byte, 1+decoder.balanceKeys.Size(key))
		encoded[0] = bankBalancePrefix
		_, encodeErr := decoder.balanceKeys.Encode(encoded[1:], key)
		require.NoError(t, encodeErr)
		return &iavl.KVPair{Key: encoded, Value: value}
	}
	changes := &iavl.ChangeSet{Pairs: []*iavl.KVPair{
		makePair(make([]byte, common.AddressLength), "ibc/foo"),
		makePair(make([]byte, 32), "basecro"),
	}}
	got, err := decoder.decodeBalances(changes)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestCompareExpectedWorldStateFindsEIP2935Shape(t *testing.T) {
	address := common.HexToAddress("0x0000F90827F1C53a10cb7A02335B175320002935")
	wireAddress := crypto.Keccak256Hash(address.Bytes())
	code := []byte{0x60, 0x00}
	codeHash := crypto.Keccak256Hash(code)
	expected := expectedWorldState{
		newAccounts: []dtypes.NewAccount{{
			Address: wireAddress, Balance: uint256.NewInt(0), CodeHash: codeHash,
		}},
		codeWrites:   []dtypes.NewCode{{CodeHash: codeHash, Code: code}},
		rawAddresses: map[common.Hash]common.Address{wireAddress: address},
	}
	available := make(map[common.Hash]struct{})
	reported := make(map[common.Hash]struct{})
	findings := compareExpectedWorldState(
		58_825_800,
		expected,
		normalizedWorldState{
			accounts: make(map[common.Hash]wireAccountState),
			deleted:  make(map[common.Hash]struct{}),
			codes:    make(map[common.Hash][]byte),
		},
		available,
		reported,
	)
	kinds := make([]string, 0, len(findings))
	for _, finding := range findings {
		kinds = append(kinds, finding.Kind)
	}
	slices.Sort(kinds)
	require.Equal(t, []string{"code_unavailable_by_height", "missing_new_account"}, kinds)
	require.Equal(t, address.Hex(), findings[0].Address)
	require.Contains(t, reported, codeHash)
}

func TestCompareExpectedWorldStateDistinguishesPreviouslyAvailableCode(t *testing.T) {
	code := []byte{0x60, 0x00}
	codeHash := crypto.Keccak256Hash(code)
	findings := compareExpectedWorldState(
		7,
		expectedWorldState{codeWrites: []dtypes.NewCode{{CodeHash: codeHash, Code: code}}},
		normalizedWorldState{
			accounts: make(map[common.Hash]wireAccountState),
			deleted:  make(map[common.Hash]struct{}),
			codes:    make(map[common.Hash][]byte),
		},
		map[common.Hash]struct{}{codeHash: {}},
		make(map[common.Hash]struct{}),
	)
	require.Equal(t, []worldAuditFinding{{
		Height: 7, Severity: auditSeverityWarning, Kind: "missing_redundant_new_code",
		CodeHash: codeHash.Hex(), Detail: "the code body was already available from an earlier height",
	}}, findings)
}

func TestNormalizeWorldStateRejectsDuplicatesAndBadCode(t *testing.T) {
	address := common.HexToHash("0x01")
	_, err := normalizeWorldState(dtypes.BlockStorageDiff{
		NewAccounts: []dtypes.NewAccount{
			{Address: address, Balance: uint256.NewInt(1)},
			{Address: address, Balance: uint256.NewInt(1)},
		},
	})
	require.ErrorContains(t, err, "duplicates")

	_, err = normalizeWorldState(dtypes.BlockStorageDiff{
		NewCodes: []dtypes.NewCode{{CodeHash: common.HexToHash("0x01"), Code: []byte{0x60}}},
	})
	require.ErrorContains(t, err, "body hash")
}
