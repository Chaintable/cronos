package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/evmos/ethermint/debank/replay"
)

var _ replay.SegmentExecutor = (*ReplayExecutor)(nil)

// ReplaySegmentHeader precedes the existing protobuf-preserving request codec.
// The same connection carries both the speculative result and its final ACK.
type ReplaySegmentHeader struct {
	Segment replay.Segment `json:"segment"`
	Height  int64          `json:"height"`
	Drop    bool           `json:"drop,omitempty"`
}

type segmentAck struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// ReadReplaySegmentDecision reads the verifier's Y. Depending on the JSON
// decoder's read-ahead, the request encoder's final newline can still precede it.
func ReadReplaySegmentDecision(r io.ByteReader) error {
	b, err := r.ReadByte()
	if err == nil && b == '\n' {
		b, err = r.ReadByte()
	}
	if err != nil {
		return err
	}
	if b != 'Y' {
		return fmt.Errorf("unexpected segment decision %q", b)
	}
	return nil
}

func WriteReplaySegmentAck(w io.Writer, err error) error {
	ack := segmentAck{OK: err == nil}
	if err != nil {
		ack.Error = err.Error()
	}
	return json.NewEncoder(w).Encode(ack)
}

func readReplaySegmentAck(r io.Reader) error {
	var ack segmentAck
	if err := json.NewDecoder(r).Decode(&ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("worker rejected segment operation: %s", ack.Error)
	}
	return nil
}

func (e *ReplayExecutor) openSegment(ctx context.Context, header ReplaySegmentHeader) (net.Conn, func(), error) {
	if err := header.Segment.Validate(header.Height); err != nil {
		return nil, nil, err
	}
	if header.Segment.Start < ReplayFirstHeight || header.Segment.End > ReplayLastHeight {
		return nil, nil, fmt.Errorf("segment outside supported replay interval")
	}
	timeout, err := replayDurationEnv("CRONOS_REPLAY_TIMEOUT_SECONDS", 10*time.Minute)
	if err != nil {
		return nil, nil, err
	}
	socket, err := e.process.ensure(e.home, e.logger)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	closeConn := func() {
		cancel()
		conn.Close()
	}
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	if _, err = conn.Write([]byte{'S'}); err == nil {
		err = json.NewEncoder(conn).Encode(header)
	}
	if err != nil {
		closeConn()
		return nil, nil, err
	}
	return conn, closeConn, nil
}

func (e *ReplayExecutor) ReplaySegmentBlock(ctx context.Context, req replay.Request, segment replay.Segment) (*replay.Result, func(bool) error, error) {
	if req.BeginBlock == nil {
		return nil, nil, fmt.Errorf("missing begin block request")
	}
	height := req.BeginBlock.Header.Height
	if err := e.CheckReplayHeight(req.ChainID, height); err != nil {
		return nil, nil, err
	}
	conn, closeConn, err := e.openSegment(ctx, ReplaySegmentHeader{Segment: segment, Height: height})
	if err != nil {
		return nil, nil, err
	}
	if err = replay.WriteRequest(conn, req); err != nil {
		closeConn()
		return nil, nil, err
	}
	result, err := replay.ReadResult(conn)
	if err != nil {
		closeConn()
		return nil, nil, err
	}
	var once sync.Once
	var finishErr error
	finish := func(verified bool) error {
		once.Do(func() {
			defer closeConn()
			if !verified {
				return
			}
			if finishErr = ctx.Err(); finishErr != nil {
				return
			}
			if _, finishErr = conn.Write([]byte{'Y'}); finishErr == nil {
				finishErr = readReplaySegmentAck(conn)
			}
		})
		return finishErr
	}
	return result, finish, nil
}

func (e *ReplayExecutor) DropReplaySegment(ctx context.Context, segment replay.Segment, height int64) error {
	conn, closeConn, err := e.openSegment(ctx, ReplaySegmentHeader{Segment: segment, Height: height, Drop: true})
	if err != nil {
		return err
	}
	defer closeConn()
	return readReplaySegmentAck(conn)
}
