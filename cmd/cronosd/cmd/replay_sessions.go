package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/crypto-org-chain/cronos/v2/app"
	"github.com/evmos/ethermint/debank/replay"
)

func readSegmentHeader(r *bufio.Reader) (app.ReplaySegmentHeader, error) {
	var h app.ReplaySegmentHeader
	if _, err := r.ReadByte(); err != nil {
		return h, err
	}
	line, err := r.ReadSlice('\n')
	if err != nil {
		return h, err
	}
	if err := json.Unmarshal(line, &h); err != nil {
		return h, err
	}
	if err := h.Segment.Validate(h.Height); err != nil {
		return h, err
	}
	if h.Segment.Start < app.ReplayFirstHeight || h.Segment.End > app.ReplayLastHeight {
		return h, fmt.Errorf("segment outside replay heights")
	}
	return h, nil
}

func checkSegmentRequest(req replay.Request, height int64) error {
	r := req.Request
	if r == nil || r.Height != height || req.ChainID != app.ReplayChainID || len(r.Hash) != 32 || len(req.ParentHash) != 32 {
		return fmt.Errorf("invalid segment block identity at height %d", height)
	}
	return nil
}

type replaySession struct {
	segment replay.Segment
	app     *app.App
	home    string
	height  int64  // Last confirmed height.
	hash    []byte // Last confirmed block hash.
	appHash []byte // AppHash committed by the last confirmed block.
	busy    bool
	idleAt  time.Time
	expiry  *time.Timer
}

type replaySessions struct {
	mu      sync.Mutex
	byID    map[string]*replaySession
	oneOff  int // Single-block requests holding an app outside byID.
	limit   int
	timeout time.Duration
}

func newReplaySessions(limit int, timeout time.Duration) *replaySessions {
	return &replaySessions{byID: make(map[string]*replaySession), limit: limit, timeout: timeout}
}

// The caller holds one execution slot. In a full registry at least one idle
// app can therefore be reclaimed before a new app is allocated.
func (p *replaySessions) acquire(seg *replay.Segment, req replay.Request) (*replaySession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if seg != nil {
		if s := p.byID[seg.ID]; s != nil {
			if s.segment != *seg || s.busy {
				return nil, fmt.Errorf("conflicting or active replay segment %q", seg.ID)
			}
			r := req.Request
			switch {
			case r.Height == s.height+1 && bytes.Equal(req.ParentHash, s.appHash):
				s.expiry.Stop()
				s.busy = true
				return s, nil
			case r.Height == s.height && bytes.Equal(r.Hash, s.hash):
				// The parent may have lost our ACK response. Recompute the
				// same block from its archived parent before accepting an ACK.
				p.forget(s)
			default:
				return nil, fmt.Errorf("unexpected parent or height %d for replay segment %q at %d", r.Height, seg.ID, s.height)
			}
		}
	}
	if len(p.byID)+p.oneOff >= p.limit {
		var oldest *replaySession
		for _, s := range p.byID {
			if !s.busy && (oldest == nil || s.idleAt.Before(oldest.idleAt)) {
				oldest = s
			}
		}
		if oldest == nil {
			return nil, fmt.Errorf("no free replay app")
		}
		p.forget(oldest)
	}
	s := &replaySession{busy: true}
	if seg == nil {
		p.oneOff++
	} else {
		s.segment = *seg
		p.byID[seg.ID] = s
	}
	return s, nil
}

// release keeps a verified segment app for the next height; appHash is the
// AppHash its last block committed.
func (p *replaySessions) release(s *replaySession, req replay.Request, appHash []byte, verified bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.segment.ID == "" {
		p.oneOff--
	}
	if !verified || req.Request.Height == s.segment.End {
		p.forget(s)
		return
	}
	s.hash = req.Request.Hash
	s.appHash = appHash
	s.height = req.Request.Height
	s.busy = false
	s.idleAt = time.Now()
	height := s.height
	s.expiry = time.AfterFunc(p.timeout, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.byID[s.segment.ID] == s && !s.busy && s.height == height {
			p.forget(s)
		}
	})
}

func (p *replaySessions) drop(seg replay.Segment, height int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.byID[seg.ID]; s != nil {
		if s.segment != seg || s.busy || height != s.height+1 {
			return fmt.Errorf("cannot drop replay segment %q at %d", seg.ID, height)
		}
		p.forget(s)
	}
	return nil
}

// Called under the registry lock only while the caller owns or evicts the app.
func (p *replaySessions) forget(s *replaySession) {
	delete(p.byID, s.segment.ID)
	if s.expiry != nil {
		s.expiry.Stop()
	}
	s.app = nil
	if s.home != "" {
		os.RemoveAll(s.home)
	}
}

func (p *replaySessions) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range p.byID {
		p.forget(s)
	}
}
