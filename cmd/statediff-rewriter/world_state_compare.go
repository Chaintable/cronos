package main

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	dtypes "github.com/evmos/ethermint/debank/types"
)

const (
	auditSeverityDefect  = "defect"
	auditSeverityWarning = "warning"
)

type worldAuditFinding struct {
	Height      int64  `json:"height"`
	Severity    string `json:"severity"`
	Kind        string `json:"kind"`
	Address     string `json:"address,omitempty"`
	WireAddress string `json:"wire_address,omitempty"`
	CodeHash    string `json:"code_hash,omitempty"`
	Expected    string `json:"expected,omitempty"`
	Actual      string `json:"actual,omitempty"`
	Detail      string `json:"detail,omitempty"`
}

type wireAccountState struct {
	Balance  [32]byte
	Nonce    uint64
	CodeHash common.Hash
}

type normalizedWorldState struct {
	accounts map[common.Hash]wireAccountState
	deleted  map[common.Hash]struct{}
	codes    map[common.Hash][]byte
}

func normalizeWorldState(diff dtypes.BlockStorageDiff) (normalizedWorldState, error) {
	result := normalizedWorldState{
		accounts: make(map[common.Hash]wireAccountState, len(diff.NewAccounts)),
		deleted:  make(map[common.Hash]struct{}, len(diff.DeletedAccounts)),
		codes:    make(map[common.Hash][]byte, len(diff.NewCodes)),
	}
	for index, account := range diff.NewAccounts {
		if account.Balance == nil {
			return result, fmt.Errorf("new account %d has nil balance", index)
		}
		if _, found := result.accounts[account.Address]; found {
			return result, fmt.Errorf("new account %d duplicates %s", index, account.Address)
		}
		var balance [32]byte
		account.Balance.WriteToSlice(balance[:])
		result.accounts[account.Address] = wireAccountState{
			Balance: balance, Nonce: account.Nonce, CodeHash: account.CodeHash,
		}
	}
	for index, address := range diff.DeletedAccounts {
		if _, found := result.deleted[address]; found {
			return result, fmt.Errorf("deleted account %d duplicates %s", index, address)
		}
		if _, found := result.accounts[address]; found {
			return result, fmt.Errorf("account %s is both new and deleted", address)
		}
		result.deleted[address] = struct{}{}
	}
	for index, code := range diff.NewCodes {
		if _, found := result.codes[code.CodeHash]; found {
			return result, fmt.Errorf("new code %d duplicates %s", index, code.CodeHash)
		}
		if actual := crypto.Keccak256Hash(code.Code); actual != code.CodeHash {
			return result, fmt.Errorf("new code %d key %s contains body hash %s", index, code.CodeHash, actual)
		}
		result.codes[code.CodeHash] = append([]byte(nil), code.Code...)
	}
	return result, nil
}

func compareExpectedWorldState(
	height int64,
	expected expectedWorldState,
	actual normalizedWorldState,
	availableCodes map[common.Hash]struct{},
	unavailableReported map[common.Hash]struct{},
) []worldAuditFinding {
	findings := make([]worldAuditFinding, 0)
	for _, account := range expected.newAccounts {
		got, found := actual.accounts[account.Address]
		if !found {
			findings = append(findings, worldAuditFinding{
				Height: height, Severity: auditSeverityDefect, Kind: "missing_new_account",
				Address: expected.rawAddresses[account.Address].Hex(), WireAddress: account.Address.Hex(),
				Expected: formatNewAccount(account),
			})
			continue
		}
		var balance [32]byte
		account.Balance.WriteToSlice(balance[:])
		want := wireAccountState{Balance: balance, Nonce: account.Nonce, CodeHash: account.CodeHash}
		if got != want {
			findings = append(findings, worldAuditFinding{
				Height: height, Severity: auditSeverityDefect, Kind: "wrong_new_account",
				Address: expected.rawAddresses[account.Address].Hex(), WireAddress: account.Address.Hex(),
				Expected: formatWireAccount(want), Actual: formatWireAccount(got),
			})
		}
		if _, deleted := actual.deleted[account.Address]; deleted {
			findings = append(findings, worldAuditFinding{
				Height: height, Severity: auditSeverityDefect, Kind: "new_account_marked_deleted",
				Address: expected.rawAddresses[account.Address].Hex(), WireAddress: account.Address.Hex(),
			})
		}
	}
	for _, address := range expected.deletedAccounts {
		if _, found := actual.deleted[address]; !found {
			findings = append(findings, worldAuditFinding{
				Height: height, Severity: auditSeverityDefect, Kind: "missing_deleted_account",
				Address: expected.rawAddresses[address].Hex(), WireAddress: address.Hex(),
			})
		}
		if _, found := actual.accounts[address]; found {
			findings = append(findings, worldAuditFinding{
				Height: height, Severity: auditSeverityDefect, Kind: "deleted_account_has_new_account",
				Address: expected.rawAddresses[address].Hex(), WireAddress: address.Hex(),
			})
		}
	}

	for _, code := range expected.codeWrites {
		got, found := actual.codes[code.CodeHash]
		if found && !bytes.Equal(got, code.Code) {
			findings = append(findings, worldAuditFinding{
				Height: height, Severity: auditSeverityDefect, Kind: "wrong_new_code",
				CodeHash: code.CodeHash.Hex(),
			})
			continue
		}
		if found {
			continue
		}
		_, available := availableCodes[code.CodeHash]
		if available {
			findings = append(findings, worldAuditFinding{
				Height: height, Severity: auditSeverityWarning, Kind: "missing_redundant_new_code",
				CodeHash: code.CodeHash.Hex(),
				Detail:   "the code body was already available from an earlier height",
			})
			continue
		}
		findings = append(findings, worldAuditFinding{
			Height: height, Severity: auditSeverityDefect, Kind: "code_unavailable_by_height",
			CodeHash: code.CodeHash.Hex(), Expected: fmt.Sprintf("%x", code.Code),
		})
		unavailableReported[code.CodeHash] = struct{}{}
	}

	for _, account := range expected.newAccounts {
		if account.CodeHash == emptyEVMCodeHash || account.CodeHash == (common.Hash{}) {
			continue
		}
		if _, found := actual.codes[account.CodeHash]; found {
			continue
		}
		if _, found := availableCodes[account.CodeHash]; found {
			continue
		}
		if _, found := unavailableReported[account.CodeHash]; found {
			continue
		}
		findings = append(findings, worldAuditFinding{
			Height: height, Severity: auditSeverityDefect, Kind: "account_code_unavailable",
			Address:     expected.rawAddresses[account.Address].Hex(),
			WireAddress: account.Address.Hex(), CodeHash: account.CodeHash.Hex(),
		})
		unavailableReported[account.CodeHash] = struct{}{}
	}
	return findings
}

func formatNewAccount(account dtypes.NewAccount) string {
	var balance [32]byte
	if account.Balance != nil {
		account.Balance.WriteToSlice(balance[:])
	}
	return formatWireAccount(wireAccountState{
		Balance: balance, Nonce: account.Nonce, CodeHash: account.CodeHash,
	})
}

func formatWireAccount(account wireAccountState) string {
	return fmt.Sprintf(
		"balance=0x%x nonce=%d code_hash=%s",
		account.Balance, account.Nonce, account.CodeHash.Hex(),
	)
}
