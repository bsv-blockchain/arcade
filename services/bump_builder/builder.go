package bump_builder

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/bump"
	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/metrics"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

type Builder struct {
	cfg      *config.Config
	logger   *zap.Logger
	store    store.Store
	producer *kafka.Producer
	consumer *kafka.ConsumerGroup
	teranode *teranode.Client
}

// New constructs a Builder. producer is the shared process-wide producer —
// the builder reuses it (for DLQ routing) rather than creating a duplicate
// connection. teranodeClient supplies the live datahub URL list (static +
// p2p-discovered, refreshed from the shared store) used for block fetches.
func New(cfg *config.Config, logger *zap.Logger, producer *kafka.Producer, st store.Store, teranodeClient *teranode.Client) *Builder {
	return &Builder{
		cfg:      cfg,
		logger:   logger.Named("bump-builder"),
		store:    st,
		producer: producer,
		teranode: teranodeClient,
	}
}

func (b *Builder) Name() string { return "bump-builder" }

func (b *Builder) Start(ctx context.Context) error {
	consumer, err := kafka.NewConsumerGroup(kafka.ConsumerConfig{
		Broker:     b.producer.Broker(),
		GroupID:    b.cfg.Kafka.ConsumerGroup + "-bump-builder",
		Topics:     []string{kafka.TopicBlockProcessed},
		Handler:    b.handleMessage,
		Producer:   b.producer,
		MaxRetries: b.cfg.Kafka.MaxRetries,
		Logger:     b.logger,
	})
	if err != nil {
		return fmt.Errorf("creating consumer group: %w", err)
	}
	b.consumer = consumer

	b.logger.Info("bump builder started",
		zap.Int("grace_window_ms", b.cfg.BumpBuilder.GraceWindowMs),
	)
	return consumer.Run(ctx)
}

func (b *Builder) Stop() error {
	b.logger.Info("stopping bump builder")
	if b.consumer != nil {
		return b.consumer.Close()
	}
	return nil
}

func (b *Builder) handleMessage(ctx context.Context, msg *kafka.Message) error {
	overallStart := time.Now()
	metrics.BumpBuilderBlocksProcessedTotal.Inc()
	// outcome reflects the terminal disposition of this BLOCK_PROCESSED. Set
	// before each return so the duration histogram lands in the right bucket.
	outcome := "success"
	defer func() {
		metrics.BumpBuilderBuildDuration.WithLabelValues(outcome).Observe(time.Since(overallStart).Seconds())
	}()

	var callback models.CallbackMessage
	if err := json.Unmarshal(msg.Value, &callback); err != nil {
		outcome = "parse_failed"
		return fmt.Errorf("unmarshaling block processed message: %w", err)
	}

	blockHash := callback.BlockHash
	if blockHash == "" {
		outcome = "parse_failed"
		return fmt.Errorf("empty block hash in block_processed message")
	}

	logger := b.logger.With(zap.String("block_hash", blockHash))

	// Grace window: merkle-service's stumpGate only waits for the first HTTP attempt
	// of each STUMP before releasing BLOCK_PROCESSED. STUMPs that got a 5xx on the
	// first attempt retry asynchronously and may land after BLOCK_PROCESSED.
	if grace := time.Duration(b.cfg.BumpBuilder.GraceWindowMs) * time.Millisecond; grace > 0 {
		metrics.BumpBuilderGraceWaitTotal.Inc()
		logger.Debug("waiting grace window", zap.Duration("duration", grace))
		select {
		case <-ctx.Done():
			outcome = "context_cancelled"
			return ctx.Err()
		case <-time.After(grace):
		}
	}

	// 1. Get all STUMPs for this block
	stumps, err := b.store.GetStumpsByBlockHash(ctx, blockHash)
	if err != nil {
		outcome = "store_failed"
		return fmt.Errorf("getting STUMPs for block: %w", err)
	}

	metrics.BumpBuilderStumpCount.Observe(float64(len(stumps)))
	if len(stumps) == 0 {
		outcome = "no_stumps"
		logger.Info("no STUMPs found — block has no tracked transactions, skipping BUMP construction")
		return nil
	}

	logger.Info("building compound BUMP", zap.Int("stump_count", len(stumps)))
	logStumpInputs(logger, stumps)

	// 2. Fetch subtree hashes + coinbase BUMP + header merkle root from datahub.
	// Pull the live URL list from the shared teranode.Client so this picks up
	// p2p-discovered URLs in addition to statically configured ones. Fall back
	// to the full set if every endpoint is currently sidelined by the circuit
	// breaker — better to retry against a sidelined URL than fail with zero
	// attempts.
	endpoints := b.teranode.GetHealthyEndpoints()
	if len(endpoints) == 0 {
		endpoints = b.teranode.GetEndpoints()
	}
	logger.Debug("fetching block data from datahub", zap.Strings("datahub_urls", endpoints))
	fetchStart := time.Now()
	subtreeHashes, coinbaseBUMP, headerMerkleRoot, err := bump.FetchBlockDataForBUMP(ctx, endpoints, blockHash, logger)
	metrics.BumpBuilderDatahubFetchDuration.Observe(time.Since(fetchStart).Seconds())
	if err != nil {
		outcome = "fetch_failed"
		return fmt.Errorf("fetching block data: %w", err)
	}
	logger.Debug("datahub fetch succeeded",
		zap.Int("subtree_count", len(subtreeHashes)),
		zap.Bool("has_coinbase_bump", coinbaseBUMP != nil),
		zap.Bool("has_header_merkle_root", headerMerkleRoot != nil),
	)
	logBlockInputs(logger, subtreeHashes, coinbaseBUMP)

	if len(subtreeHashes) == 0 {
		logger.Warn("block has no subtrees, cannot construct BUMPs")
		return nil
	}

	// Per-STUMP assembled paths (before merge) — useful for spotting which subtree
	// contributed a wrong element to the compound BUMP.
	logPerStumpAssembly(logger, stumps, subtreeHashes, coinbaseBUMP)

	// 3. Build compound BUMP (STUMPs are sparse — only for subtrees with tracked txs)
	compound, txids, err := bump.BuildCompoundBUMP(stumps, subtreeHashes, coinbaseBUMP)
	if err != nil {
		return fmt.Errorf("building compound BUMP: %w", err)
	}

	blockHeight := uint64(compound.BlockHeight)
	bumpBytes := compound.Bytes()
	logCompoundBUMP(logger, compound, bumpBytes, txids)

	// 4. Validate: compound BUMP root must match the block header's merkle root.
	// A mismatch means the compound is malformed (missing siblings, wrong offsets,
	// stale subtree roots, …). Refuse to persist so clients never see a BUMP that
	// fails ComputeRoot, and leave txs non-MINED + STUMPs intact so a retry can
	// rebuild once the inputs are correct.
	if err := bump.ValidateCompoundRoot(compound, headerMerkleRoot); err != nil {
		dumpBUMPFailureInputs(logger, stumps, subtreeHashes, coinbaseBUMP, headerMerkleRoot, compound, bumpBytes, txids, err)
		return fmt.Errorf("compound BUMP root mismatch for block %s: %w", blockHash, err)
	}

	// 5. Store compound BUMP as binary
	if err := b.store.InsertBUMP(ctx, blockHash, blockHeight, bumpBytes); err != nil {
		return fmt.Errorf("storing BUMP: %w", err)
	}

	// 6. Set tracked transactions to MINED
	if len(txids) > 0 {
		if _, err := b.store.SetMinedByTxIDs(ctx, blockHash, txids); err != nil {
			logger.Error("failed to set mined status", zap.Error(err))
		} else {
			logger.Info("set transactions to MINED",
				zap.Int("count", len(txids)),
			)
		}
	}

	// 7. Prune STUMPs
	if err := b.store.DeleteStumpsByBlockHash(ctx, blockHash); err != nil {
		logger.Warn("failed to clean up STUMPs", zap.Error(err))
	}

	logger.Info("BUMP built successfully",
		zap.Int("tracked_txids", len(txids)),
		zap.Int("stumps_pruned", len(stumps)),
	)
	return nil
}

// --- Debug helpers ---
//
// These emit at Debug level only (enabled via log_level=debug). They dump the
// raw inputs and intermediate artifacts of BUMP construction so a human can
// replay the math offline and compare against expected values.

// logStumpInputs logs each stored STUMP: its subtree index, the raw BRC-74
// bytes, and the level-0 hashes (candidate txids in that subtree).
func logStumpInputs(logger *zap.Logger, stumps []*models.Stump) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	for _, s := range stumps {
		leaves := bump.ExtractLevel0Hashes(s.StumpData)
		leafHex := make([]string, len(leaves))
		for i, h := range leaves {
			leafHex[i] = h.String()
		}
		logger.Debug("stump input",
			zap.Int("subtree_index", s.SubtreeIndex),
			zap.Int("stump_bytes", len(s.StumpData)),
			zap.String("stump_hex", hex.EncodeToString(s.StumpData)),
			zap.Int("level0_count", len(leaves)),
			zap.Strings("level0_hashes", leafHex),
		)
	}
}

// logBlockInputs dumps the datahub-provided subtree hashes and coinbase BUMP
// that feed into compound construction.
func logBlockInputs(logger *zap.Logger, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	subtreeHex := make([]string, len(subtreeHashes))
	for i, h := range subtreeHashes {
		subtreeHex[i] = h.String()
	}
	logger.Debug("block inputs",
		zap.Int("subtree_count", len(subtreeHashes)),
		zap.Strings("subtree_hashes", subtreeHex),
	)
	if len(coinbaseBUMP) > 0 {
		cbPath, err := transaction.NewMerklePathFromBinary(coinbaseBUMP)
		var cbTxID string
		if err == nil && len(cbPath.Path) > 0 {
			for _, e := range cbPath.Path[0] {
				if e.Offset == 0 && e.Hash != nil {
					cbTxID = e.Hash.String()
					break
				}
			}
		}
		logger.Debug("coinbase bump",
			zap.Int("bytes", len(coinbaseBUMP)),
			zap.String("hex", hex.EncodeToString(coinbaseBUMP)),
			zap.String("coinbase_txid", cbTxID),
		)
	}
}

// logPerStumpAssembly expands each STUMP into its full-block merkle path in
// isolation and logs the per-level path elements. The compound BUMP is the
// deduped union of these — a wrong element here is a wrong element there.
func logPerStumpAssembly(logger *zap.Logger, stumps []*models.Stump, subtreeHashes []chainhash.Hash, coinbaseBUMP []byte) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	for _, s := range stumps {
		full, _, err := bump.AssembleBUMP(s.StumpData, s.SubtreeIndex, subtreeHashes, coinbaseBUMP)
		if err != nil {
			logger.Debug("per-stump assembly failed",
				zap.Int("subtree_index", s.SubtreeIndex),
				zap.Error(err),
			)
			continue
		}
		logger.Debug("per-stump assembly",
			zap.Int("subtree_index", s.SubtreeIndex),
			zap.Uint32("block_height", full.BlockHeight),
			zap.Int("levels", len(full.Path)),
			zap.String("full_bump_hex", hex.EncodeToString(full.Bytes())),
		)
		for level, elems := range full.Path {
			logger.Debug("per-stump level",
				zap.Int("subtree_index", s.SubtreeIndex),
				zap.Int("level", level),
				zap.String("elements", formatPathElements(elems)),
			)
		}
	}
}

// logCompoundBUMP dumps the final merged BUMP: raw bytes, per-level structure,
// and all tracked txids.
func logCompoundBUMP(logger *zap.Logger, compound *transaction.MerklePath, bumpBytes []byte, txids []string) {
	if !logger.Core().Enabled(zap.DebugLevel) {
		return
	}
	logger.Debug("compound bump",
		zap.Uint32("block_height", compound.BlockHeight),
		zap.Int("levels", len(compound.Path)),
		zap.Int("bytes", len(bumpBytes)),
		zap.String("hex", hex.EncodeToString(bumpBytes)),
		zap.Int("txid_count", len(txids)),
		zap.Strings("txids", txids),
	)
	for level, elems := range compound.Path {
		logger.Debug("compound bump level",
			zap.Int("level", level),
			zap.Int("element_count", len(elems)),
			zap.String("elements", formatPathElements(elems)),
		)
	}
}

// dumpBUMPFailureInputs emits an ERROR-level event with every input needed to
// replay a failed compound BUMP build offline: raw STUMP bytes (hex) per subtree,
// subtree hashes, coinbase BUMP, block-header merkle root, the final compound
// BUMP bytes, and per-level offsets of the compound. Always emits regardless of
// configured log level — this fires only when validation fails, so it doesn't
// contribute to normal-path noise.
func dumpBUMPFailureInputs(
	logger *zap.Logger,
	stumps []*models.Stump,
	subtreeHashes []chainhash.Hash,
	coinbaseBUMP []byte,
	headerMerkleRoot *chainhash.Hash,
	compound *transaction.MerklePath,
	compoundBytes []byte,
	txids []string,
	validationErr error,
) {
	stumpDumps := make([]string, len(stumps))
	for i, s := range stumps {
		stumpDumps[i] = fmt.Sprintf("subtree=%d bytes=%d hex=%s",
			s.SubtreeIndex, len(s.StumpData), hex.EncodeToString(s.StumpData))
	}
	subtreeHex := make([]string, len(subtreeHashes))
	for i, h := range subtreeHashes {
		subtreeHex[i] = h.String()
	}
	levelDumps := make([]string, 0, len(compound.Path))
	for level, elems := range compound.Path {
		levelDumps = append(levelDumps,
			fmt.Sprintf("level=%d count=%d elems=[%s]", level, len(elems), formatPathElements(elems)))
	}

	var headerRootHex string
	if headerMerkleRoot != nil {
		headerRootHex = headerMerkleRoot.String()
	}

	logger.Error("compound BUMP validation failed — refusing to persist",
		zap.Error(validationErr),
		zap.String("header_merkle_root", headerRootHex),
		zap.Int("stump_count", len(stumps)),
		zap.Strings("stumps", stumpDumps),
		zap.Int("subtree_count", len(subtreeHashes)),
		zap.Strings("subtree_hashes", subtreeHex),
		zap.Int("coinbase_bump_bytes", len(coinbaseBUMP)),
		zap.String("coinbase_bump_hex", hex.EncodeToString(coinbaseBUMP)),
		zap.Int("compound_bytes", len(compoundBytes)),
		zap.String("compound_hex", hex.EncodeToString(compoundBytes)),
		zap.Int("compound_levels", len(compound.Path)),
		zap.Strings("compound_by_level", levelDumps),
		zap.Int("txid_count", len(txids)),
		zap.Strings("txids", txids),
	)
}

// formatPathElements renders a slice of PathElements as a human-readable string
// "offset=42 hash=abc… txid=true duplicate=false; offset=43 …".
func formatPathElements(elems []*transaction.PathElement) string {
	if len(elems) == 0 {
		return ""
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		var hashStr string
		if e.Hash != nil {
			hashStr = e.Hash.String()
		}
		txid := false
		if e.Txid != nil {
			txid = *e.Txid
		}
		dup := false
		if e.Duplicate != nil {
			dup = *e.Duplicate
		}
		parts = append(parts, fmt.Sprintf("offset=%d hash=%s txid=%v duplicate=%v", e.Offset, hashStr, txid, dup))
	}
	return strings.Join(parts, "; ")
}
