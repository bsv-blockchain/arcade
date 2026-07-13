package finality

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
)

// fakeReader is a ChainReader with programmable results and call counters.
type fakeReader struct {
	tip          *chaintracks.BlockHeader
	headers      []*chaintracks.BlockHeader
	headersErr   error
	blockOnCtx   bool
	tipCalls     int
	headersCalls int
}

func (f *fakeReader) GetTip(ctx context.Context) *chaintracks.BlockHeader {
	f.tipCalls++
	if f.blockOnCtx {
		<-ctx.Done()
		return nil
	}
	return f.tip
}

func (f *fakeReader) GetHeaders(_ context.Context, _, _ uint32) ([]*chaintracks.BlockHeader, error) {
	f.headersCalls++
	return f.headers, f.headersErr
}

func header(height uint32, timestamp uint32) *chaintracks.BlockHeader {
	h := &chaintracks.BlockHeader{
		Header: &block.Header{Timestamp: timestamp},
		Height: height,
	}
	h.Hash = chainhash.HashH([]byte{byte(height), byte(height >> 8), byte(height >> 16), byte(timestamp)})
	return h
}

// chainAt builds a reader whose tip is at the given height with the given
// timestamps for the last len(timestamps) blocks ending at the tip.
func chainAt(tipHeight uint32, timestamps ...uint32) *fakeReader {
	headers := make([]*chaintracks.BlockHeader, len(timestamps))
	for i, ts := range timestamps {
		headers[i] = header(tipHeight-uint32(len(timestamps)-1-i), ts)
	}
	return &fakeReader{tip: headers[len(headers)-1], headers: headers}
}

// nonFinalTx needs chain state: timestamp locktime with a non-final sequence.
func nonFinalTx(lockTime uint32) *sdkTx.Transaction {
	return txWith(lockTime, 0xfffffffe)
}

func TestCheckerComputesBIP113MedianOfLast11(t *testing.T) {
	// Unsorted timestamps; sorted median (index 5 of 11) is 5000.
	reader := chainAt(957457,
		9000, 1000, 5000, 3000, 7000, 2000, 8000, 4000, 6000, 500, 9500)
	c := NewChecker(reader, nil)

	err := c.Check(context.Background(), nonFinalTx(2_000_000_000))
	var nfe *NotFinalError
	if !errors.As(err, &nfe) {
		t.Fatalf("Check() = %v, want *NotFinalError", err)
	}
	if nfe.MedianTimePast != 5000 {
		t.Errorf("MedianTimePast = %d, want 5000", nfe.MedianTimePast)
	}
	if nfe.NextBlockHeight != 957458 {
		t.Errorf("NextBlockHeight = %d, want 957458", nfe.NextBlockHeight)
	}
}

func TestCheckerShortChainUsesAvailableHeaders(t *testing.T) {
	// Tip height 4: only 5 headers exist; median is sorted index 2.
	reader := chainAt(4, 300, 100, 500, 200, 400)
	c := NewChecker(reader, nil)

	err := c.Check(context.Background(), nonFinalTx(2_000_000_000))
	var nfe *NotFinalError
	if !errors.As(err, &nfe) {
		t.Fatalf("Check() = %v, want *NotFinalError", err)
	}
	if nfe.MedianTimePast != 300 {
		t.Errorf("MedianTimePast = %d, want 300", nfe.MedianTimePast)
	}
}

func TestCheckerFinalVerdictAgainstFetchedMTP(t *testing.T) {
	reader := chainAt(100,
		1_600_000_000, 1_600_000_001, 1_600_000_002, 1_600_000_003, 1_600_000_004, 1_600_000_005,
		1_600_000_006, 1_600_000_007, 1_600_000_008, 1_600_000_009, 1_600_000_010)
	c := NewChecker(reader, nil)

	// Median is 1_600_000_005; a timestamp locktime below it is final.
	if err := c.Check(context.Background(), nonFinalTx(1_600_000_004)); err != nil {
		t.Fatalf("Check() = %v, want nil (locktime below fetched MTP)", err)
	}
}

func TestCheckerFastPathSkipsChainIO(t *testing.T) {
	reader := chainAt(100, 1000)
	c := NewChecker(reader, nil)

	if err := c.Check(context.Background(), txWith(2_000_000_000, 0xffffffff)); err != nil {
		t.Fatalf("all-final-sequence tx: Check() = %v, want nil", err)
	}
	if err := c.Check(context.Background(), txWith(0, 0xfffffffe)); err != nil {
		t.Fatalf("locktime-zero tx: Check() = %v, want nil", err)
	}
	if reader.tipCalls != 0 || reader.headersCalls != 0 {
		t.Errorf("chain I/O on fast path: tipCalls=%d headersCalls=%d, want 0/0", reader.tipCalls, reader.headersCalls)
	}
}

func TestCheckerCachesWithinTTL(t *testing.T) {
	reader := chainAt(100, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010)
	c := NewChecker(reader, nil, WithTTL(time.Hour))

	_ = c.Check(context.Background(), nonFinalTx(2_000_000_000))
	_ = c.Check(context.Background(), nonFinalTx(2_000_000_000))

	if reader.tipCalls != 1 || reader.headersCalls != 1 {
		t.Errorf("tipCalls=%d headersCalls=%d, want 1/1 (second check served from cache)", reader.tipCalls, reader.headersCalls)
	}
}

func TestCheckerTTLExpiryUnchangedTipReusesMTP(t *testing.T) {
	reader := chainAt(100, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010)
	c := NewChecker(reader, nil, WithTTL(0))

	_ = c.Check(context.Background(), nonFinalTx(2_000_000_000))
	_ = c.Check(context.Background(), nonFinalTx(2_000_000_000))

	if reader.tipCalls != 2 {
		t.Errorf("tipCalls = %d, want 2 (TTL expired)", reader.tipCalls)
	}
	if reader.headersCalls != 1 {
		t.Errorf("headersCalls = %d, want 1 (tip unchanged, MTP reused)", reader.headersCalls)
	}
}

func TestCheckerTipChangeRecomputesMTP(t *testing.T) {
	reader := chainAt(100, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010)
	c := NewChecker(reader, nil, WithTTL(0))

	err := c.Check(context.Background(), nonFinalTx(2_000_000_000))
	var nfe *NotFinalError
	if !errors.As(err, &nfe) || nfe.MedianTimePast != 1005 {
		t.Fatalf("first check: %v, want MTP 1005", err)
	}

	// New tip: reorg to a chain whose MTP moved backwards.
	next := chainAt(101, 900, 901, 902, 903, 904, 905, 906, 907, 908, 909, 910)
	reader.tip = next.tip
	reader.headers = next.headers

	err = c.Check(context.Background(), nonFinalTx(2_000_000_000))
	if !errors.As(err, &nfe) || nfe.MedianTimePast != 905 {
		t.Fatalf("after tip change: %v, want MTP 905", err)
	}
	if nfe.NextBlockHeight != 102 {
		t.Errorf("NextBlockHeight = %d, want 102", nfe.NextBlockHeight)
	}
	if reader.headersCalls != 2 {
		t.Errorf("headersCalls = %d, want 2 (recompute on tip change)", reader.headersCalls)
	}
}

func TestCheckerFailsOpen(t *testing.T) {
	valid := chainAt(100, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010)

	tests := []struct {
		name   string
		reader *fakeReader
	}{
		{"nil tip", &fakeReader{tip: nil}},
		{"headers error", &fakeReader{tip: valid.tip, headersErr: errors.New("boom")}},
		{"empty headers", &fakeReader{tip: valid.tip, headers: nil}},
		{"zero MTP", chainAt(100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unavailable := 0
			c := NewChecker(tt.reader, nil, WithUnavailableHook(func() { unavailable++ }))
			if err := c.Check(context.Background(), nonFinalTx(2_000_000_000)); err != nil {
				t.Fatalf("Check() = %v, want nil (fail-open)", err)
			}
			if unavailable != 1 {
				t.Errorf("unavailable hook calls = %d, want 1", unavailable)
			}
		})
	}
}

func TestCheckerFetchBudget(t *testing.T) {
	reader := &fakeReader{blockOnCtx: true}
	c := NewChecker(reader, nil, WithFetchTimeout(50*time.Millisecond))

	start := time.Now()
	err := c.Check(context.Background(), nonFinalTx(2_000_000_000))
	if err != nil {
		t.Fatalf("Check() = %v, want nil (fail-open on timeout)", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Check took %v, want bounded by fetch timeout", elapsed)
	}
}

func TestCheckerNilReceiverIsNoop(t *testing.T) {
	var c *Checker
	if err := c.Check(context.Background(), nonFinalTx(2_000_000_000)); err != nil {
		t.Fatalf("nil checker: Check() = %v, want nil", err)
	}
}

func TestNewCheckerNilReaderYieldsNil(t *testing.T) {
	if c := NewChecker(nil, nil); c != nil {
		t.Fatalf("NewChecker(nil, ...) = %v, want nil", c)
	}
}
