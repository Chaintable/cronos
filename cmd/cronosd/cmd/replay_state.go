//go:build rocksdb

package cmd

import (
	"bytes"
	"fmt"

	iavlstore "github.com/cosmos/cosmos-sdk/store/v2/iavl"
	storetypes "github.com/cosmos/cosmos-sdk/store/v2/types"
	"github.com/cosmos/iavl"
	"github.com/crypto-org-chain/cronos/app"
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
