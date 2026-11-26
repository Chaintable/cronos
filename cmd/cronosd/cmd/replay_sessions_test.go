//go:build replay
// +build replay

package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/crypto-org-chain/cronos/app"
	"github.com/stretchr/testify/require"
	abci "github.com/tendermint/tendermint/abci/types"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tharsis/ethermint/debank/replay"
)

func segmentTestBlock(height int64) replay.Request {
	hash := func(h int64) []byte {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(h))
		sum := sha256.Sum256(b[:])
		return sum[:]
	}
	parentHash := bytes.Repeat([]byte{1}, 32)
	return replay.Request{ChainID: app.ReplayChainID, ParentHash: parentHash, BeginBlock: &abci.RequestBeginBlock{
		Hash: hash(height), Header: tmproto.Header{Height: height, ChainID: app.ReplayChainID, AppHash: parentHash,
			LastBlockId: tmproto.BlockID{Hash: hash(height - 1)}},
	}}
}

func TestReplaySegmentVerifiedHandoff(t *testing.T) {
	p := newReplaySessions(2, time.Minute)
	defer p.close()
	seg := replay.Segment{ID: "A", Start: app.ReplayFirstHeight, End: app.ReplayFirstHeight + 2}
	first, next, last := segmentTestBlock(seg.Start), segmentTestBlock(seg.Start+1), segmentTestBlock(seg.End)
	s, err := p.acquire(&seg, first)
	require.NoError(t, err)
	a := new(app.App)
	s.app = a
	_, err = p.acquire(&seg, next)
	require.Error(t, err, "cannot use results until verified")
	require.Error(t, p.drop(seg, seg.Start+1), "cannot steal a busy app")
	p.release(s, first, true)
	_, err = p.acquire(&seg, last)
	require.Error(t, err, "out of order")
	wrong := segmentTestBlock(seg.Start + 1)
	wrong.BeginBlock.Header.LastBlockId.Hash[0] ^= 1
	_, err = p.acquire(&seg, wrong)
	require.Error(t, err, "parent identity mismatch")
	require.Error(t, p.drop(seg, seg.Start), "stale drop")
	resumed, err := p.acquire(&seg, next)
	require.NoError(t, err)
	require.Same(t, a, resumed.app)
	p.release(resumed, next, false)
	retry, err := p.acquire(&seg, next)
	require.NoError(t, err)
	require.Nil(t, retry.app, "aborted app must be discarded")
	p.release(retry, next, true)
	retry, err = p.acquire(&seg, next)
	require.NoError(t, err, "lost ACK response can replay committed height")
	require.Nil(t, retry.app)
	p.release(retry, next, true)
	resumed, err = p.acquire(&seg, last)
	require.NoError(t, err)
	p.release(resumed, last, true)
	require.Empty(t, p.byID)
}

func TestReplaySegmentCapacityExpiryAndDrop(t *testing.T) {
	p := newReplaySessions(1, time.Minute)
	defer p.close()
	seg := replay.Segment{ID: "A", Start: app.ReplayFirstHeight, End: app.ReplayFirstHeight + 3}
	first := segmentTestBlock(seg.Start)
	s, err := p.acquire(&seg, first)
	require.NoError(t, err)
	s.app = new(app.App)
	p.release(s, first, true)
	legacy, err := p.acquire(nil, first)
	require.NoError(t, err)
	require.Nil(t, s.app, "legacy request can reclaim an idle session")
	require.Equal(t, 1, p.oneOff)
	p.release(legacy, first, false)
	require.Zero(t, p.oneOff)
	s, err = p.acquire(&seg, first)
	require.NoError(t, err)
	p.release(s, first, true)
	require.NoError(t, p.drop(seg, seg.Start+1))
	require.Empty(t, p.byID)

	p.timeout = time.Millisecond
	s, err = p.acquire(&seg, first)
	require.NoError(t, err)
	p.release(s, first, true)
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.byID) == 0
	}, time.Second, time.Millisecond)
}
