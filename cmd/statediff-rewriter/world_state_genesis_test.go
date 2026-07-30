package main

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestCronosGenesisExpectedWorldStateUsesVersionOneProjection(t *testing.T) {
	projection := newWorldStateProjection()
	for index, address := range cronosGenesisAccounts {
		var balance [32]byte
		balance[31] = byte(index + 1)
		projection.accounts[address] = authState{
			Nonce: uint64(index + 10), CodeHash: emptyEVMCodeHash,
		}
		projection.balances[address] = balance
	}

	expected, err := cronosGenesisExpectedWorldState(&archiveReader{}, projection, 0)
	require.NoError(t, err)
	require.Len(t, expected.newAccounts, len(cronosGenesisAccounts))
	require.Empty(t, expected.deletedAccounts)
	require.Empty(t, expected.codeWrites)
	require.Empty(t, expected.codeDeletes)

	byWireAddress := make(map[common.Hash]wireAccountState, len(expected.newAccounts))
	for _, account := range expected.newAccounts {
		var balance [32]byte
		account.Balance.WriteToSlice(balance[:])
		byWireAddress[account.Address] = wireAccountState{
			Balance: balance, Nonce: account.Nonce, CodeHash: account.CodeHash,
		}
	}
	for index, address := range cronosGenesisAccounts {
		wireAddress := crypto.Keccak256Hash(address.Bytes())
		account, found := byWireAddress[wireAddress]
		require.True(t, found)
		require.Equal(t, byte(index+1), account.Balance[31])
		require.Equal(t, uint64(index+10), account.Nonce)
		require.Equal(t, emptyEVMCodeHash, account.CodeHash)
		require.Equal(t, address, expected.rawAddresses[wireAddress])
	}
}

func TestCronosGenesisExpectedWorldStateRejectsMissingAccount(t *testing.T) {
	projection := newWorldStateProjection()
	for _, address := range cronosGenesisAccounts[1:] {
		projection.accounts[address] = authState{CodeHash: emptyEVMCodeHash}
	}
	_, err := cronosGenesisExpectedWorldState(&archiveReader{}, projection, 0)
	require.ErrorContains(t, err, cronosGenesisAccounts[0].Hex())
}

func TestCronosGenesisOutcomeRequiresExactSets(t *testing.T) {
	address := cronosGenesisAccounts[0]
	wireAddress := crypto.Keccak256Hash(address.Bytes())
	expected := expectedWorldState{
		newAccounts: []dtypes.NewAccount{{
			Address: wireAddress, Balance: new(uint256.Int), CodeHash: emptyEVMCodeHash,
		}},
		rawAddresses: map[common.Hash]common.Address{wireAddress: address},
	}
	actual := normalizedWorldState{
		accounts: map[common.Hash]wireAccountState{
			wireAddress:                {CodeHash: emptyEVMCodeHash},
			common.HexToHash("0x1234"): {CodeHash: emptyEVMCodeHash},
		},
		deleted: map[common.Hash]struct{}{common.HexToHash("0x5678"): {}},
		codes:   map[common.Hash][]byte{},
	}
	findings := cronosGenesisOutcomeFindings(
		worldAuditFetch{task: worldAuditTask{height: 1, expected: expected}, actual: actual},
		map[common.Hash]struct{}{},
		map[common.Hash]struct{}{},
	)
	require.Equal(t, []string{
		"unexpected_genesis_new_account",
		"unexpected_genesis_deleted_account",
	}, []string{findings[0].Kind, findings[1].Kind})
}
