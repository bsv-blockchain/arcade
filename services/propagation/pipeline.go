package propagation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/config"
	"github.com/bsv-blockchain/arcade/kafka"
	"github.com/bsv-blockchain/arcade/merkleservice"
	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
	"github.com/bsv-blockchain/arcade/teranode"
)

// Pipeline is the dep-aware propagation service. It wires together
// three goroutines that cooperate via channels:
//
//   - dispatcherConsumer: reads Kafka messages off TopicDispatch,
//     defers offset commits until each tx terminalizes
//   - Dispatcher: maintains the in-flight set / waiter index / retry
//     queue / batch accumulator
//   - dispatcherBroadcaster: submits batches to Teranode, writes
//     terminal statuses to the store, emits per-tx statusFlips back
//     to the dispatcher
//
// Replaces the legacy parallel-consumer Propagator. Both still exist
// in the source tree during the rolling cutover; app.BuildServices
// decides which one to register.
//
// Lifecycle matches services.Service: Start blocks on the consumer's
// Run for the dispatcher's lifetime; Stop cancels the internal
// context and waits for every goroutine to exit cleanly.
type Pipeline struct {
	cfg            *config.Config
	logger         *zap.Logger
	producer       *kafka.Producer
	store          store.Store
	teranodeClient *teranode.Client
	merkleClient   *merkleservice.Client

	// Channels — the wiring between dispatcher / consumer / broadcaster.
	// Buffered to absorb short bursts; sizes match the typical batch
	// shape so a producer-side spike doesn't stall a consumer-side
	// drain.
	incomingMsgs  chan dispatcherMsg
	outgoingBatch chan []*inFlightEntry
	statusFlips   chan statusFlip

	// Components.
	dispatcher  *Dispatcher
	consumer    *dispatcherConsumer
	broadcaster *dispatcherBroadcaster

	// Lifecycle.
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
}

// NewPipeline constructs a Pipeline with all components wired but not
// started. Defaults applied where config is zero:
//
//   - max_in_flight       = 100000 (caps dep-index memory)
//   - batch_max_size      = TeranodeMaxBatchSize, default 100
//   - batch_flush_timeout = 50ms
//   - retry_max_attempts  = 5, matches existing reaper budget
//   - retry_backoff_ms    = 500, matches existing reaper base
//   - commit_ticker       = 200ms
func NewPipeline(
	cfg *config.Config,
	logger *zap.Logger,
	producer *kafka.Producer,
	st store.Store,
	tc *teranode.Client,
	mc *merkleservice.Client,
) (*Pipeline, error) {
	if cfg == nil {
		return nil, fmt.Errorf("propagation.NewPipeline: cfg required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if producer == nil {
		return nil, fmt.Errorf("propagation.NewPipeline: producer required")
	}
	if st == nil {
		return nil, fmt.Errorf("propagation.NewPipeline: store required")
	}
	if tc == nil {
		return nil, fmt.Errorf("propagation.NewPipeline: teranodeClient required")
	}

	logger = logger.Named("propagation")

	batchMax := cfg.Propagation.TeranodeMaxBatchSize
	if batchMax <= 0 {
		batchMax = 100
	}
	retryMax := cfg.Propagation.RetryMaxAttempts
	if retryMax <= 0 {
		retryMax = 5
	}
	retryBackoff := cfg.Propagation.RetryBackoffMs
	if retryBackoff <= 0 {
		retryBackoff = 500
	}

	// Channel buffers — sized so transient producer/consumer rate
	// differences don't immediately block the chain. The dispatcher's
	// max_in_flight is the real backpressure surface.
	incoming := make(chan dispatcherMsg, batchMax)
	outgoing := make(chan []*inFlightEntry, 4)
	flips := make(chan statusFlip, batchMax)

	offsets := newOffsetTracker()

	disp := NewDispatcher(
		logger.Named("dispatcher"),
		incoming,
		outgoing,
		flips,
		offsets,
		100000,       // maxInFlight
		batchMax,     // batchMaxSize
		retryMax,     // retryMaxAttempts
		retryBackoff, // retryBackoffMs
		50*time.Millisecond,
	)

	groupID := cfg.Kafka.ConsumerGroup + "-dispatcher"

	cons, err := newDispatcherConsumer(dispatcherConsumerConfig{
		Broker:       producer.Broker(),
		GroupID:      groupID,
		Topic:        kafka.TopicDispatch,
		Dispatcher:   disp,
		Offsets:      offsets,
		IncomingMsgs: incoming,
		CommitTicker: 200 * time.Millisecond,
		Logger:       logger.Named("consumer"),
	})
	if err != nil {
		return nil, fmt.Errorf("propagation.NewPipeline: consumer: %w", err)
	}

	bcast, err := newDispatcherBroadcaster(dispatcherBroadcasterConfig{
		TeranodeClient:      tc,
		Store:               st,
		Incoming:            outgoing,
		Flips:               flips,
		Logger:              logger.Named("broadcaster"),
		SubmitTimeout:       15 * time.Second,
		MerkleClient:        mc,
		MerkleConcurrency:   cfg.Propagation.MerkleConcurrency,
		MerkleCallbackURL:   cfg.CallbackURL,
		MerkleCallbackToken: cfg.CallbackToken,
	})
	if err != nil {
		return nil, fmt.Errorf("propagation.NewPipeline: broadcaster: %w", err)
	}

	// Wire the dispatcher's rejected-sink to the broadcaster's store
	// write path so cascade rejections from REJECTED parents also
	// produce a terminal store row. The sink runs synchronously in the
	// dispatcher goroutine, so we hand off through a non-blocking send
	// onto the flips channel — keeps the dispatcher loop fast and
	// guarantees the store write rides the broadcaster's existing
	// per-flip handler logic.
	disp.SetRejectedSink(func(txid, reason string) {
		select {
		case flips <- statusFlip{
			txid:       txid,
			status:     statusRejectedForCascade(),
			errorMsg:   reason,
			statusCode: 0,
		}:
		default:
			// flips is full — the dispatcher will eventually drain it
			// and process this rejection via the in-flight set
			// recovery on the next status flip for any related tx.
			// Cascade thoroughness is best-effort under sustained
			// backpressure; correctness is preserved by Kafka replay.
			logger.Warn("dispatcher: rejected sink dropped, flips channel full",
				zap.String("txid", txid))
		}
	})

	return &Pipeline{
		cfg:            cfg,
		logger:         logger,
		producer:       producer,
		store:          st,
		teranodeClient: tc,
		merkleClient:   mc,
		incomingMsgs:   incoming,
		outgoingBatch:  outgoing,
		statusFlips:    flips,
		dispatcher:     disp,
		consumer:       cons,
		broadcaster:    bcast,
	}, nil
}

// Name implements services.Service. Matches the legacy Propagator's
// name so log/metric labels are consistent during rollout.
func (p *Pipeline) Name() string { return "propagation" }

// Start launches the broadcaster and dispatcher goroutines, then runs
// the consumer in the calling goroutine (blocking until ctx is
// canceled or the subscription closes). Returns the consumer's exit
// error, if any. The internal cancel function is captured so Stop can
// terminate the background goroutines independently.
func (p *Pipeline) Start(ctx context.Context) error {
	innerCtx, cancel := context.WithCancel(ctx)
	p.cancelFunc = cancel

	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		p.broadcaster.Run(innerCtx)
	}()
	go func() {
		defer p.wg.Done()
		p.dispatcher.Run(innerCtx)
	}()

	p.logger.Info("propagation pipeline started",
		zap.String("topic", kafka.TopicDispatch),
		zap.String("group_id", p.cfg.Kafka.ConsumerGroup+"-dispatcher"),
	)

	// Consumer blocks here until shutdown.
	return p.consumer.Run(innerCtx)
}

// Stop cancels the internal context, which cascades to the broadcaster
// and dispatcher goroutines via their context arguments. Waits for
// both to exit before returning. The consumer is expected to exit on
// its own when ctx is canceled — Stop doesn't wait on it explicitly
// because Start's caller blocks on Run() return.
func (p *Pipeline) Stop() error {
	p.logger.Info("stopping propagation pipeline")
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
	p.wg.Wait()
	return nil
}

// statusRejectedForCascade returns the models.StatusRejected constant.
// Pulled into a tiny helper because the dispatcher_broadcaster.go file
// references models.Status for the flip's Status field — keeping the
// pipeline file free of the models import unless needed lets it stay
// focused on wiring.
//
// (Indirection is throwaway — once the cascade path moves to its own
// dedicated handler outside the broadcaster, this helper goes away.)
func statusRejectedForCascade() models.Status { return models.StatusRejected }
