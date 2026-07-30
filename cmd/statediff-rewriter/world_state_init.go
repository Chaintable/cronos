package main

import (
	"fmt"

	"github.com/cosmos/iavl"
	"github.com/ethereum/go-ethereum/common"
)

type worldStateInitialization struct {
	Version      int64 `json:"version"`
	Accounts     int   `json:"accounts"`
	Balances     int   `json:"balances"`
	GenesisCodes int   `json:"genesis_codes"`
}

func initializeWorldStateProjection(
	archive *archiveReader,
	decoder *worldStateDecoder,
	version int64,
	cacheSize int,
) (*worldStateProjection, map[common.Hash]struct{}, worldStateInitialization, error) {
	return initializeWorldStateProjectionWithCodes(archive, decoder, version, cacheSize, true)
}

func initializeAccountProjection(
	archive *archiveReader,
	decoder *worldStateDecoder,
	version int64,
	cacheSize int,
) (*worldStateProjection, worldStateInitialization, error) {
	projection, _, report, err := initializeWorldStateProjectionWithCodes(
		archive, decoder, version, cacheSize, false,
	)
	return projection, report, err
}

func initializeWorldStateProjectionWithCodes(
	archive *archiveReader,
	decoder *worldStateDecoder,
	version int64,
	cacheSize int,
	includeCodes bool,
) (*worldStateProjection, map[common.Hash]struct{}, worldStateInitialization, error) {
	if archive == nil || decoder == nil || version < 1 {
		return nil, nil, worldStateInitialization{}, fmt.Errorf("archive, decoder, and positive version are required")
	}
	projection := newWorldStateProjection()
	availableCodes := make(map[common.Hash]struct{})
	report := worldStateInitialization{Version: version}

	accountTree, err := archive.versionedStateSource("acc", cacheSize).GetImmutable(version)
	if err != nil {
		return nil, nil, report, fmt.Errorf("load acc IAVL version %d: %w", version, err)
	}
	err = iterateImmutablePrefix(accountTree, []byte{0x01}, []byte{0x02}, func(key, value []byte) error {
		mutations, err := decoder.decodeAccounts(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{
			Key: append([]byte(nil), key...), Value: append([]byte(nil), value...),
		}}})
		if err != nil {
			return err
		}
		for _, mutation := range mutations {
			projection.accounts[mutation.Address] = mutation.State
			report.Accounts++
		}
		return nil
	})
	if err != nil {
		return nil, nil, report, fmt.Errorf("initialize accounts at version %d: %w", version, err)
	}

	balanceTree, err := archive.versionedStateSource("bank", cacheSize).GetImmutable(version)
	if err != nil {
		return nil, nil, report, fmt.Errorf("load bank IAVL version %d: %w", version, err)
	}
	err = iterateImmutablePrefix(balanceTree, []byte{0x02}, []byte{0x03}, func(key, value []byte) error {
		mutations, err := decoder.decodeBalances(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{
			Key: append([]byte(nil), key...), Value: append([]byte(nil), value...),
		}}})
		if err != nil {
			return err
		}
		for _, mutation := range mutations {
			if mutation.Balance != ([32]byte{}) {
				projection.balances[mutation.Address] = mutation.Balance
				report.Balances++
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, report, fmt.Errorf("initialize balances at version %d: %w", version, err)
	}
	if !includeCodes {
		return projection, availableCodes, report, nil
	}

	codeTree, err := archive.versionedStateSource("evm", cacheSize).GetImmutable(version)
	if err != nil {
		return nil, nil, report, fmt.Errorf("load evm IAVL version %d: %w", version, err)
	}
	err = iterateImmutablePrefix(codeTree, []byte{0x01}, []byte{0x02}, func(key, value []byte) error {
		mutations, err := decodeCodes(&iavl.ChangeSet{Pairs: []*iavl.KVPair{{
			Key: append([]byte(nil), key...), Value: append([]byte(nil), value...),
		}}})
		if err != nil {
			return err
		}
		for _, mutation := range mutations {
			availableCodes[mutation.CodeHash] = struct{}{}
			report.GenesisCodes++
		}
		return nil
	})
	if err != nil {
		return nil, nil, report, fmt.Errorf("initialize codes at version %d: %w", version, err)
	}
	return projection, availableCodes, report, nil
}

func iterateImmutablePrefix(
	tree *iavl.ImmutableTree,
	start, end []byte,
	callback func([]byte, []byte) error,
) error {
	if tree == nil || callback == nil {
		return fmt.Errorf("immutable tree and callback are required")
	}
	iterator, err := tree.Iterator(start, end, true)
	if err != nil {
		return err
	}
	defer iterator.Close()
	for ; iterator.Valid(); iterator.Next() {
		if err := callback(iterator.Key(), iterator.Value()); err != nil {
			return err
		}
	}
	return iterator.Error()
}
