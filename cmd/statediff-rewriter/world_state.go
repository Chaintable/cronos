package main

import (
	"bytes"
	"fmt"
	"sort"

	"cosmossdk.io/collections"
	collcodec "cosmossdk.io/collections/codec"
	sdkcodec "github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	vestingtypes "github.com/cosmos/cosmos-sdk/x/auth/vesting/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/evmos/ethermint/encoding"
	etherminttypes "github.com/evmos/ethermint/types"
	"github.com/holiman/uint256"

	dtypes "github.com/evmos/ethermint/debank/types"
)

const (
	authAccountPrefix = byte(0x01)
	bankBalancePrefix = byte(0x02)
	evmCodePrefix     = byte(0x01)
)

var emptyEVMCodeHash = crypto.Keccak256Hash(nil)

type authState struct {
	Nonce    uint64
	CodeHash common.Hash
}

type accountMutation struct {
	Address common.Address
	Delete  bool
	State   authState
}

type balanceMutation struct {
	Address common.Address
	Delete  bool
	Balance [32]byte
}

type codeMutation struct {
	CodeHash common.Hash
	Delete   bool
	Code     []byte
}

type worldStateDelta struct {
	height   int64
	accounts []accountMutation
	balances []balanceMutation
	codes    []codeMutation
}

type worldStateDecoder struct {
	accountValues collcodec.ValueCodec[sdk.AccountI]
	balanceKeys   collcodec.KeyCodec[collections.Pair[sdk.AccAddress, string]]
	evmDenom      string
}

func newWorldStateDecoder(evmDenom string) (*worldStateDecoder, error) {
	if evmDenom == "" {
		return nil, fmt.Errorf("EVM denom is required")
	}
	config := encoding.MakeConfig()
	authtypes.RegisterInterfaces(config.InterfaceRegistry)
	vestingtypes.RegisterInterfaces(config.InterfaceRegistry)
	return &worldStateDecoder{
		accountValues: sdkcodec.CollInterfaceValue[sdk.AccountI](config.Codec),
		balanceKeys:   collections.PairKeyCodec(sdk.AccAddressKey, collections.StringKey),
		evmDenom:      evmDenom,
	}, nil
}

func (decoder *worldStateDecoder) decodeAccounts(changeSet *iavl.ChangeSet) ([]accountMutation, error) {
	if decoder == nil || changeSet == nil {
		return nil, fmt.Errorf("account decoder and changeset are required")
	}
	result := make([]accountMutation, 0, len(changeSet.Pairs))
	seen := make(map[common.Address]struct{}, len(changeSet.Pairs))
	for pairIndex, pair := range changeSet.Pairs {
		if pair == nil || len(pair.Key) < 2 || pair.Key[0] != authAccountPrefix {
			return nil, fmt.Errorf("account pair %d has invalid key %x", pairIndex, pair.GetKey())
		}
		if len(pair.Key) != 1+common.AddressLength {
			continue
		}
		address := common.BytesToAddress(pair.Key[1:])
		if _, found := seen[address]; found {
			return nil, fmt.Errorf("account pair %d duplicates address %s", pairIndex, address)
		}
		seen[address] = struct{}{}
		mutation := accountMutation{Address: address, Delete: pair.Delete}
		if pair.Delete {
			if len(pair.Value) != 0 {
				return nil, fmt.Errorf("deleted account %s has a value", address)
			}
			result = append(result, mutation)
			continue
		}
		if len(pair.Value) == 0 {
			return nil, fmt.Errorf("account %s has an empty value", address)
		}
		account, err := decoder.accountValues.Decode(pair.Value)
		if err != nil {
			return nil, fmt.Errorf("decode account %s: %w", address, err)
		}
		if account == nil {
			return nil, fmt.Errorf("account %s decoded to nil", address)
		}
		codeHash := emptyEVMCodeHash
		if ethAccount, ok := account.(etherminttypes.EthAccountI); ok {
			codeHash = ethAccount.GetCodeHash()
			if codeHash == (common.Hash{}) {
				codeHash = emptyEVMCodeHash
			}
		}
		mutation.State = authState{Nonce: account.GetSequence(), CodeHash: codeHash}
		result = append(result, mutation)
	}
	return result, nil
}

func (decoder *worldStateDecoder) decodeBalances(changeSet *iavl.ChangeSet) ([]balanceMutation, error) {
	if decoder == nil || changeSet == nil {
		return nil, fmt.Errorf("balance decoder and changeset are required")
	}
	result := make([]balanceMutation, 0, len(changeSet.Pairs))
	seen := make(map[common.Address]struct{}, len(changeSet.Pairs))
	for pairIndex, pair := range changeSet.Pairs {
		if pair == nil || len(pair.Key) < 2 || pair.Key[0] != bankBalancePrefix {
			return nil, fmt.Errorf("balance pair %d has invalid key %x", pairIndex, pair.GetKey())
		}
		read, key, err := decoder.balanceKeys.Decode(pair.Key[1:])
		if err != nil {
			return nil, fmt.Errorf("decode balance pair %d key: %w", pairIndex, err)
		}
		if read != len(pair.Key)-1 {
			return nil, fmt.Errorf("balance pair %d key has %d trailing bytes", pairIndex, len(pair.Key)-1-read)
		}
		if key.K2() != decoder.evmDenom {
			continue
		}
		rawAddress := key.K1()
		if len(rawAddress) != common.AddressLength {
			continue
		}
		address := common.BytesToAddress(rawAddress)
		if _, found := seen[address]; found {
			return nil, fmt.Errorf("balance pair %d duplicates address %s", pairIndex, address)
		}
		seen[address] = struct{}{}
		mutation := balanceMutation{Address: address, Delete: pair.Delete}
		if pair.Delete {
			if len(pair.Value) != 0 {
				return nil, fmt.Errorf("deleted balance %s has a value", address)
			}
			result = append(result, mutation)
			continue
		}
		if len(pair.Value) == 0 {
			return nil, fmt.Errorf("balance %s has an empty value", address)
		}
		amount, err := banktypes.BalanceValueCodec.Decode(pair.Value)
		if err != nil {
			return nil, fmt.Errorf("decode balance %s: %w", address, err)
		}
		if amount.IsNegative() || amount.BigInt().BitLen() > 256 {
			return nil, fmt.Errorf("balance %s is outside uint256", address)
		}
		amount.BigInt().FillBytes(mutation.Balance[:])
		result = append(result, mutation)
	}
	return result, nil
}

func decodeCodes(changeSet *iavl.ChangeSet) ([]codeMutation, error) {
	if changeSet == nil {
		return nil, fmt.Errorf("code changeset is required")
	}
	result := make([]codeMutation, 0, len(changeSet.Pairs))
	seen := make(map[common.Hash]struct{}, len(changeSet.Pairs))
	for pairIndex, pair := range changeSet.Pairs {
		if pair == nil || len(pair.Key) != 1+common.HashLength || pair.Key[0] != evmCodePrefix {
			return nil, fmt.Errorf("code pair %d has invalid key %x", pairIndex, pair.GetKey())
		}
		codeHash := common.BytesToHash(pair.Key[1:])
		if _, found := seen[codeHash]; found {
			return nil, fmt.Errorf("code pair %d duplicates hash %s", pairIndex, codeHash)
		}
		seen[codeHash] = struct{}{}
		mutation := codeMutation{CodeHash: codeHash, Delete: pair.Delete}
		if pair.Delete {
			if len(pair.Value) != 0 {
				return nil, fmt.Errorf("deleted code %s has a value", codeHash)
			}
			result = append(result, mutation)
			continue
		}
		if len(pair.Value) == 0 {
			return nil, fmt.Errorf("code %s has an empty body", codeHash)
		}
		if actual := crypto.Keccak256Hash(pair.Value); actual != codeHash {
			return nil, fmt.Errorf("code key %s contains body hash %s", codeHash, actual)
		}
		mutation.Code = append([]byte(nil), pair.Value...)
		result = append(result, mutation)
	}
	return result, nil
}

type projectedAccount struct {
	Present  bool
	Balance  [32]byte
	Nonce    uint64
	CodeHash common.Hash
}

type worldStateProjection struct {
	accounts map[common.Address]authState
	balances map[common.Address][32]byte
}

func newWorldStateProjection() *worldStateProjection {
	return &worldStateProjection{
		accounts: make(map[common.Address]authState),
		balances: make(map[common.Address][32]byte),
	}
}

func (projection *worldStateProjection) account(address common.Address) projectedAccount {
	auth, hasAuth := projection.accounts[address]
	balance, hasBalance := projection.balances[address]
	result := projectedAccount{
		Present:  hasAuth || hasBalance,
		Balance:  balance,
		CodeHash: emptyEVMCodeHash,
	}
	if hasAuth {
		result.Nonce = auth.Nonce
		result.CodeHash = auth.CodeHash
	}
	return result
}

func sameProjectedAccountValues(left, right projectedAccount) bool {
	return left.Balance == right.Balance &&
		left.Nonce == right.Nonce &&
		left.CodeHash == right.CodeHash
}

type expectedWorldState struct {
	newAccounts     []dtypes.NewAccount
	deletedAccounts []common.Hash
	codeWrites      []dtypes.NewCode
	codeDeletes     []common.Hash
	rawAddresses    map[common.Hash]common.Address
}

func (projection *worldStateProjection) apply(delta worldStateDelta) expectedWorldState {
	addresses := make(map[common.Address]projectedAccount, len(delta.accounts)+len(delta.balances))
	for _, mutation := range delta.accounts {
		addresses[mutation.Address] = projection.account(mutation.Address)
	}
	for _, mutation := range delta.balances {
		if _, found := addresses[mutation.Address]; !found {
			addresses[mutation.Address] = projection.account(mutation.Address)
		}
	}
	for _, mutation := range delta.accounts {
		if mutation.Delete {
			delete(projection.accounts, mutation.Address)
		} else {
			projection.accounts[mutation.Address] = mutation.State
		}
	}
	for _, mutation := range delta.balances {
		if mutation.Delete || mutation.Balance == ([32]byte{}) {
			delete(projection.balances, mutation.Address)
		} else {
			projection.balances[mutation.Address] = mutation.Balance
		}
	}

	expected := expectedWorldState{rawAddresses: make(map[common.Hash]common.Address, len(addresses))}
	for address, before := range addresses {
		after := projection.account(address)
		// Presence alone is not observable in the EVM state represented by this
		// stream: an empty auth account and an absent account both have zero
		// balance/nonce and the empty code hash.
		if sameProjectedAccountValues(before, after) {
			continue
		}
		wireAddress := crypto.Keccak256Hash(address.Bytes())
		expected.rawAddresses[wireAddress] = address
		if !after.Present {
			expected.deletedAccounts = append(expected.deletedAccounts, wireAddress)
			continue
		}
		balance := new(uint256.Int)
		balance.SetBytes(after.Balance[:])
		expected.newAccounts = append(expected.newAccounts, dtypes.NewAccount{
			Address: wireAddress, Balance: balance, Nonce: after.Nonce, CodeHash: after.CodeHash,
		})
	}
	for _, mutation := range delta.codes {
		if mutation.Delete {
			expected.codeDeletes = append(expected.codeDeletes, mutation.CodeHash)
			continue
		}
		expected.codeWrites = append(expected.codeWrites, dtypes.NewCode{
			CodeHash: mutation.CodeHash, Code: append([]byte(nil), mutation.Code...),
		})
	}
	sort.Slice(expected.newAccounts, func(i, j int) bool {
		return bytes.Compare(expected.newAccounts[i].Address[:], expected.newAccounts[j].Address[:]) < 0
	})
	sort.Slice(expected.deletedAccounts, func(i, j int) bool {
		return bytes.Compare(expected.deletedAccounts[i][:], expected.deletedAccounts[j][:]) < 0
	})
	sort.Slice(expected.codeWrites, func(i, j int) bool {
		return bytes.Compare(expected.codeWrites[i].CodeHash[:], expected.codeWrites[j].CodeHash[:]) < 0
	})
	sort.Slice(expected.codeDeletes, func(i, j int) bool {
		return bytes.Compare(expected.codeDeletes[i][:], expected.codeDeletes[j][:]) < 0
	})
	return expected
}
