package main

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"

	dtypes "github.com/evmos/ethermint/debank/types"
)

func cronosGenesisExpectedWorldState(
	archive *archiveReader,
	projection *worldStateProjection,
	cacheSize int,
) (expectedWorldState, error) {
	if archive == nil || projection == nil {
		return expectedWorldState{}, fmt.Errorf("archive and genesis projection are required")
	}
	expected := expectedWorldState{
		newAccounts:  make([]dtypes.NewAccount, 0, len(cronosGenesisAccounts)),
		rawAddresses: make(map[common.Hash]common.Address, len(cronosGenesisAccounts)),
	}
	var codeTree codeGetter
	for _, address := range cronosGenesisAccounts {
		account := projection.account(address)
		if !account.Present {
			return expectedWorldState{}, fmt.Errorf("cronos genesis account %s is absent at IAVL version 1", address)
		}
		wireAddress := crypto.Keccak256Hash(address.Bytes())
		balance := new(uint256.Int)
		balance.SetBytes(account.Balance[:])
		expected.newAccounts = append(expected.newAccounts, dtypes.NewAccount{
			Address:  wireAddress,
			Balance:  balance,
			Nonce:    account.Nonce,
			CodeHash: account.CodeHash,
		})
		expected.rawAddresses[wireAddress] = address
		if account.CodeHash == emptyEVMCodeHash || account.CodeHash == (common.Hash{}) {
			continue
		}
		if codeTree == nil {
			tree, err := archive.versionedStateSource("evm", cacheSize).GetImmutable(1)
			if err != nil {
				return expectedWorldState{}, fmt.Errorf("load genesis EVM IAVL: %w", err)
			}
			codeTree = tree
		}
		key := append([]byte{evmCodePrefix}, account.CodeHash[:]...)
		code, err := codeTree.Get(key)
		if err != nil {
			return expectedWorldState{}, fmt.Errorf("load genesis code %s: %w", account.CodeHash, err)
		}
		if len(code) == 0 {
			return expectedWorldState{}, fmt.Errorf("genesis account %s references unavailable code %s", address, account.CodeHash)
		}
		if got := crypto.Keccak256Hash(code); got != account.CodeHash {
			return expectedWorldState{}, fmt.Errorf(
				"genesis code %s contains body hash %s",
				account.CodeHash, got,
			)
		}
		expected.codeWrites = append(expected.codeWrites, dtypes.NewCode{
			CodeHash: account.CodeHash,
			Code:     append([]byte(nil), code...),
		})
	}
	sort.Slice(expected.newAccounts, func(i, j int) bool {
		return bytes.Compare(expected.newAccounts[i].Address[:], expected.newAccounts[j].Address[:]) < 0
	})
	sort.Slice(expected.codeWrites, func(i, j int) bool {
		return bytes.Compare(expected.codeWrites[i].CodeHash[:], expected.codeWrites[j].CodeHash[:]) < 0
	})
	return expected, nil
}

type codeGetter interface {
	Get([]byte) ([]byte, error)
}
