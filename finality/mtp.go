package finality

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"
)

// mtpWindow is the number of block timestamps whose median forms the BIP113
// median-time-past.
const mtpWindow = 11

const (
	defaultTTL          = 15 * time.Second
	defaultFetchTimeout = time.Second
)

// ChainReader is the slice of go-chaintracks used to derive the finality
// comparison values. Both the embedded ChainManager and the remote HTTP
// client satisfy it.
type ChainReader interface {
	GetTip(ctx context.Context) *chaintracks.BlockHeader
	GetHeaders(ctx context.Context, height, count uint32) ([]*chaintracks.BlockHeader, error)
}

// CheckerOption configures a Checker.
type CheckerOption func(*Checker)

// WithTTL sets how long a fetched tip/MTP snapshot is trusted before the tip
// is re-read.
func WithTTL(ttl time.Duration) CheckerOption {
	return func(c *Checker) { c.ttl = ttl }
}

// WithFetchTimeout bounds the total chain-state fetch per Check call; on
// expiry the check fails open.
func WithFetchTimeout(d time.Duration) CheckerOption {
	return func(c *Checker) { c.fetchTimeout = d }
}

// WithUnavailableHook registers a callback invoked each time chain state
// could not be obtained and the check failed open (metrics hook).
func WithUnavailableHook(fn func()) CheckerOption {
	return func(c *Checker) { c.onUnavailable = fn }
}

type snapshot struct {
	tipHash   chainhash.Hash
	tipHeight uint32
	mtp       uint32
	fetchedAt time.Time
}

// Checker evaluates transaction finality against cached chain state. It is
// safe for concurrent use and nil-receiver-safe: a nil *Checker accepts every
// transaction, so callers need no wiring guards.
//
// The verdict uses arcade's view of the tip, which can trail (or, briefly,
// lead) the view of the teranode that ultimately admits the transaction.
// Divergence is confined to roughly one block around the finality boundary
// and self-heals: a false accept falls through to today's generic network
// rejection, a false reject clears on resubmission.
type Checker struct {
	reader        ChainReader
	logger        *zap.Logger
	ttl           time.Duration
	fetchTimeout  time.Duration
	onUnavailable func()

	mu   sync.Mutex
	snap snapshot
}

// NewChecker builds a Checker over the given chain source. A nil reader
// yields a nil Checker (checks disabled, everything accepted).
func NewChecker(reader ChainReader, logger *zap.Logger, opts ...CheckerOption) *Checker {
	if reader == nil {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	c := &Checker{
		reader:       reader,
		logger:       logger,
		ttl:          defaultTTL,
		fetchTimeout: defaultFetchTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Check returns a *NotFinalError when tx is provably non-final against the
// current chain state, nil otherwise. Transactions decidable without chain
// state (all input sequences final, or nLockTime zero) never trigger chain
// I/O. When chain state cannot be obtained the check fails open — teranode
// remains the authority; this gate only exists to give submitters a usable
// error message.
func (c *Checker) Check(ctx context.Context, tx *sdkTx.Transaction) error {
	if c == nil {
		return nil
	}

	if lockTimeIrrelevant(tx) {
		return nil
	}

	tipHeight, mtp, ok := c.chainState(ctx)
	if !ok {
		return nil
	}

	return IsTransactionFinal(tx, tipHeight+1, mtp)
}

// lockTimeIrrelevant reports whether finality is decidable as "final" without
// chain state.
func lockTimeIrrelevant(tx *sdkTx.Transaction) bool {
	if tx.LockTime == 0 {
		return true
	}
	for _, input := range tx.Inputs {
		if input.SequenceNumber != sdkTx.DefaultSequenceNumber {
			return false
		}
	}
	return true
}

// chainState returns the tip height and MTP, from cache when fresh. ok is
// false when chain state is unavailable (fail open).
func (c *Checker) chainState(ctx context.Context) (tipHeight, mtp uint32, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.snap.fetchedAt.IsZero() && time.Since(c.snap.fetchedAt) < c.ttl {
		return c.snap.tipHeight, c.snap.mtp, true
	}

	ctx, cancel := context.WithTimeout(ctx, c.fetchTimeout)
	defer cancel()

	tip := c.reader.GetTip(ctx)
	if tip == nil {
		return c.unavailable("chain tip unavailable", nil)
	}

	// MTP only changes when the tip does; on an unchanged tip just refresh
	// the snapshot's age. Keying on the hash (not height) also catches
	// reorgs, where the MTP can move in either direction.
	if tip.Hash == c.snap.tipHash && !c.snap.fetchedAt.IsZero() {
		c.snap.fetchedAt = time.Now()
		return c.snap.tipHeight, c.snap.mtp, true
	}

	start := uint32(0)
	count := tip.Height + 1
	if tip.Height >= mtpWindow-1 {
		start = tip.Height - (mtpWindow - 1)
		count = mtpWindow
	}
	headers, err := c.reader.GetHeaders(ctx, start, count)
	if err != nil {
		return c.unavailable("chain headers unavailable", err)
	}
	if len(headers) == 0 {
		return c.unavailable("chain returned no headers", nil)
	}

	timestamps := make([]uint32, 0, len(headers))
	for _, h := range headers {
		if h == nil || h.Header == nil {
			continue
		}
		timestamps = append(timestamps, h.Timestamp)
	}
	if len(timestamps) == 0 {
		return c.unavailable("chain returned no header timestamps", nil)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
	median := timestamps[len(timestamps)/2]
	if median == 0 {
		return c.unavailable("chain median-time-past is zero", nil)
	}

	c.snap = snapshot{tipHash: tip.Hash, tipHeight: tip.Height, mtp: median, fetchedAt: time.Now()}
	return c.snap.tipHeight, c.snap.mtp, true
}

func (c *Checker) unavailable(reason string, err error) (uint32, uint32, bool) {
	c.logger.Warn("finality pre-check skipped: "+reason, zap.Error(err))
	if c.onUnavailable != nil {
		c.onUnavailable()
	}
	return 0, 0, false
}
