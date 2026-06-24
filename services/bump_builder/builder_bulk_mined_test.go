package bump_builder

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/arcade/models"
)

// chunkCapturingPublisher records the TxIDs of each PublishBulk call verbatim
// (without unfanning) so a test can assert on chunk count and per-chunk size.
type chunkCapturingPublisher struct {
	mu     sync.Mutex
	chunks [][]string
}

func (p *chunkCapturingPublisher) Publish(context.Context, *models.TransactionStatus) error {
	return nil
}

func (p *chunkCapturingPublisher) PublishBulk(_ context.Context, template *models.TransactionStatus) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	chunk := make([]string, len(template.TxIDs))
	copy(chunk, template.TxIDs)
	p.chunks = append(p.chunks, chunk)
	return nil
}

func (p *chunkCapturingPublisher) Subscribe(context.Context, string) (<-chan *models.TransactionStatus, error) {
	return nil, errors.New("chunkCapturingPublisher: Subscribe not used in tests")
}
func (p *chunkCapturingPublisher) Close() error { return nil }

// A MINED fan-out larger than maxTxIDsPerBulkEvent is split across multiple bulk
// events so no single event exceeds the Kafka producer's max message size. A
// ~27k-tx block previously serialized to ~1.85 MB — over the 1 MiB default
// Producer.MaxMessageBytes — so PublishBulk failed and the MINED event was
// silently dropped for large blocks (the DB status was still MINED).
func TestBuilder_MarkMinedAndPublish_ChunksLargeTxidList(t *testing.T) {
	ms := newMockStore()
	pub := &chunkCapturingPublisher{}
	b := newTestBuilder(ms, "http://unused.invalid")
	b.publisher = pub

	const n = maxTxIDsPerBulkEvent*2 + 37
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("tx%064d", i)
	}

	b.markMinedAndPublish(context.Background(), zap.NewNop(), "blkD", 4242, txids)

	wantChunks := (n + maxTxIDsPerBulkEvent - 1) / maxTxIDsPerBulkEvent
	if len(pub.chunks) != wantChunks {
		t.Fatalf("expected %d bulk events, got %d", wantChunks, len(pub.chunks))
	}
	total := 0
	for i, c := range pub.chunks {
		if len(c) == 0 || len(c) > maxTxIDsPerBulkEvent {
			t.Fatalf("chunk %d has invalid size %d (cap %d)", i, len(c), maxTxIDsPerBulkEvent)
		}
		total += len(c)
	}
	if total != n {
		t.Fatalf("chunks must cover every txid exactly once: got %d, want %d", total, n)
	}
}
