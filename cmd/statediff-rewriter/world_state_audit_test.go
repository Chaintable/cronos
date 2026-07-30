package main

import (
	"context"
	"testing"

	storetypes "cosmossdk.io/store/types"
	"github.com/ethereum/go-ethereum/common"
	dtypes "github.com/evmos/ethermint/debank/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestWorldStateAuditRejectsNonGenesisStartWithoutPartialMode(t *testing.T) {
	_, err := runWorldStateAudit(context.Background(), worldAuditOptions{
		ArchiveHome: "/archive",
		Output:      t.TempDir(),
		Bucket:      "bucket",
		Prefix:      "prefix",
		Region:      "region",
		EVMDenom:    "basecro",
		FirstHeight: 3,
	}, nil)
	require.ErrorContains(t, err, "must start at height 2")
}

func TestNextWorldAuditRootsUsesGenesisRootAtHeightTwo(t *testing.T) {
	info := &storetypes.CommitInfo{
		Version: 1,
		StoreInfos: []storetypes.StoreInfo{{
			Name: "evm", CommitId: storetypes.CommitID{Version: 1, Hash: common.HexToHash("0x1234").Bytes()},
		}},
	}
	reader := &fakeCommitInfoReader{infos: map[int64]*storetypes.CommitInfo{1: info}}
	cursor, err := newArchiveRootCursor(reader, 2)
	require.NoError(t, err)
	gotRoot, gotParent, err := nextWorldAuditRoots(cursor, 2)
	require.NoError(t, err)
	require.Equal(t, common.BytesToHash(info.Hash()), gotRoot)
	require.Equal(t, cronosGenesisStateRoot, gotParent)
}

func TestMakeWorldAuditTaskCandidateRules(t *testing.T) {
	root := common.HexToHash("0x01")
	parent := common.HexToHash("0x02")
	empty, err := makeWorldAuditTask(7, root, parent, "25/prefix", expectedWorldState{})
	require.NoError(t, err)
	require.True(t, empty.skip)
	codeDelete, err := makeWorldAuditTask(7, root, parent, "25/prefix", expectedWorldState{
		codeDeletes: []common.Hash{{3}},
	})
	require.NoError(t, err)
	require.True(t, codeDelete.skip)

	for name, expected := range map[string]expectedWorldState{
		"account": {
			newAccounts: []dtypes.NewAccount{{Balance: new(uint256.Int)}},
		},
		"delete": {deletedAccounts: []common.Hash{{1}}},
		"code":   {codeWrites: []dtypes.NewCode{{CodeHash: common.Hash{2}, Code: []byte{0x60}}}},
	} {
		t.Run(name, func(t *testing.T) {
			task, err := makeWorldAuditTask(7, root, parent, "25/prefix", expected)
			require.NoError(t, err)
			require.False(t, task.skip)
			require.Equal(t, "25/prefix/0x0000000000000000000000000000000000000000000000000000000000000001/stateDiff", task.key)
		})
	}

	equalRoot, err := makeWorldAuditTask(7, root, root, "25/prefix", expectedWorldState{})
	require.NoError(t, err)
	require.True(t, equalRoot.skip)
	_, err = makeWorldAuditTask(7, root, root, "25/prefix", expectedWorldState{
		newAccounts: []dtypes.NewAccount{{Balance: new(uint256.Int)}},
	})
	require.ErrorContains(t, err, "equal app roots")
}
