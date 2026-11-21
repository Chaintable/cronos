//go:build rocksdb

package cmd

import (
	"bytes"
	"fmt"
	"github.com/evmos/ethermint/debank/statediff"
	wire "github.com/evmos/ethermint/debank/types"
	"sync"

	iavlstore "cosmossdk.io/store/iavl"
	storetypes "cosmossdk.io/store/types"
	"github.com/cosmos/iavl"
	"github.com/crypto-org-chain/cronos/v2/app"
)

// Verify the inputs to CanonicalStateAt against the final working trees.
// The reverse check catches a committed key missing from the canonical diff.
func verifyReplayChanges(a *app.App, height int64, keys []storetypes.StoreKey) error {
	written := make(map[string]map[string]bool, len(keys))
	for _, key := range keys {
		written[key.Name()] = make(map[string]bool)
	}
	for _, p := range a.CommitMultiStore().PopStateCache() {
		written[p.StoreKey][string(p.Key)] = true
	}
	for _, key := range keys {
		st, ok := a.CommitMultiStore().GetStore(key).(*iavlstore.Store)
		if !ok {
			return fmt.Errorf("%s is not IAVL", key.Name())
		}
		parent, err := st.GetImmutable(height - 1)
		if err != nil {
			return fmt.Errorf("%s parent state: %w", key.Name(), err)
		}
		seen := false
		err = st.TraverseStateChanges(height, height, func(version int64, set *iavl.ChangeSet) error {
			if version != height || set == nil || seen {
				return fmt.Errorf("unexpected change set at %d", version)
			}
			seen = true
			for _, p := range set.Pairs {
				got := st.Get(p.Key)
				if (got == nil) != p.Delete || !bytes.Equal(got, p.Value) {
					return fmt.Errorf("key %X differs from replay", p.Key)
				}
				delete(written[key.Name()], string(p.Key))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("%s change set: %w", key.Name(), err)
		}
		if !seen {
			return fmt.Errorf("%s change set missing at %d", key.Name(), height)
		}
		for k := range written[key.Name()] {
			old, now := parent.Get([]byte(k)), st.Get([]byte(k))
			if (old == nil) != (now == nil) || !bytes.Equal(old, now) {
				return fmt.Errorf("%s key %X missing in canonical change set", key.Name(), []byte(k))
			}
		}
	}
	return nil
}

// The secondary contains the committed trees for both H-1 and H. The single
// worker owns these IAVL handles throughout validation and conversion.
type replayVersion int64

func (v replayVersion) LatestVersion() int64 { return int64(v) }
func replayStateDiff(a *app.App, h int64, denom string) (*wire.TransactionStateDiff, error) {
	storeAt := func(name string) (*iavlstore.Store, error) {
		s, ok := a.CommitMultiStore().GetStore(a.GetKey(name)).(*iavlstore.Store)
		if !ok {
			return nil, fmt.Errorf("historical %s store does not expose IAVL", name)
		}
		return s, nil
	}
	accounts, err := storeAt("acc")
	if err != nil {
		return nil, err
	}
	balances, err := storeAt("bank")
	if err != nil {
		return nil, err
	}
	evm, err := storeAt("evm")
	if err != nil {
		return nil, err
	}
	source := statediff.NewIAVLStateChangeSource(new(sync.Mutex), replayVersion(h), a.AppCodec(), accounts, balances, evm)
	diff, err := statediff.CanonicalStateAt(source, h, denom)
	return &diff, err
}
