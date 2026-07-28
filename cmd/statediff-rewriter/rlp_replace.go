package main

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/rlp"
	dtypes "github.com/evmos/ethermint/debank/types"
)

const (
	blockStorageDiffFieldCount = 6
	newAccountsFieldIndex      = 2
	deletedAccountsFieldIndex  = 3
	storageDiffFieldIndex      = 4
	newCodesFieldIndex         = 5
)

func blockStorageDiffRawFields(body []byte) ([]rlp.RawValue, error) {
	var fields []rlp.RawValue
	if err := rlp.DecodeBytes(body, &fields); err != nil {
		return nil, err
	}
	if len(fields) != blockStorageDiffFieldCount {
		return nil, fmt.Errorf("block storage diff has %d RLP fields, want %d", len(fields), blockStorageDiffFieldCount)
	}
	return fields, nil
}

func replaceStorageDiffRLP(body []byte, storage []dtypes.AccountStorageDiff) ([]byte, error) {
	fields, err := blockStorageDiffRawFields(body)
	if err != nil {
		return nil, err
	}
	storageBody, err := rlp.EncodeToBytes(storage)
	if err != nil {
		return nil, err
	}
	fields[storageDiffFieldIndex] = rlp.RawValue(storageBody)
	return rlp.EncodeToBytes(fields)
}

func replaceWorldStateRLP(
	body []byte,
	world expectedWorldState,
	selected refillFields,
) ([]byte, error) {
	fields, err := blockStorageDiffRawFields(body)
	if err != nil {
		return nil, err
	}
	replacements := []struct {
		index int
		field refillFields
		value any
	}{
		{newAccountsFieldIndex, refillNewAccounts, world.newAccounts},
		{deletedAccountsFieldIndex, refillDeletedAccounts, world.deletedAccounts},
		{storageDiffFieldIndex, refillStorage, world.storage},
		{newCodesFieldIndex, refillNewCodes, world.codeWrites},
	}
	for _, replacement := range replacements {
		if !selected.has(replacement.field) {
			continue
		}
		encoded, err := rlp.EncodeToBytes(replacement.value)
		if err != nil {
			return nil, err
		}
		fields[replacement.index] = rlp.RawValue(encoded)
	}
	return rlp.EncodeToBytes(fields)
}

func verifyRawRetainedFields(oldBody, newBody []byte) error {
	oldFields, err := blockStorageDiffRawFields(oldBody)
	if err != nil {
		return fmt.Errorf("decode old raw fields: %w", err)
	}
	newFields, err := blockStorageDiffRawFields(newBody)
	if err != nil {
		return fmt.Errorf("decode new raw fields: %w", err)
	}
	for index := range oldFields {
		if index == storageDiffFieldIndex {
			continue
		}
		if !bytes.Equal(oldFields[index], newFields[index]) {
			return fmt.Errorf("retained RLP field %d changed", index)
		}
	}
	return nil
}
