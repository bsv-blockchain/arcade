package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Blob storage: GridFS files behind a manifest.
//
// GridFS gives us chunked storage for payloads above the 16 MB document cap,
// but its files document is keyed by a fresh ObjectID, not by block, so
// "the current BUMP for block H" needs a level of indirection. Each bucket
// has a manifest collection whose document is keyed by block (bumps) or by
// block+subtree (stumps) and points at the file id currently in force.
//
// Write protocol: upload the file, then atomically swap the manifest to it
// with findAndModify returning the previous reference, then delete only
// that previous file. The swap is the linearization point, and because each
// writer deletes exactly what it replaced, two concurrent rebuilds of one
// block converge on one surviving file — the last swap wins — instead of
// pruning each other's upload. Readers resolve manifest → file; a reader
// that loses a race between the two steps re-reads the manifest once.
//
// A writer that dies between upload and swap leaves an unreferenced file.
// The next successful write for the same key sweeps files older than
// staleUploadAge that it does not reference; concurrent writers' uploads
// are seconds old and are never touched.

// Metadata keys stored on GridFS files documents (queried as metadata.<key>).
const (
	metaBlockHash    = "block_hash"
	metaBlockHeight  = "block_height"
	metaSubtreeIndex = "subtree_index"

	// staleUploadAge is how old an unreferenced upload must be before a
	// later write for the same key sweeps it as a crash leftover.
	staleUploadAge = time.Hour

	// manifestSwapAttempts bounds the retry of an upserting swap that lost
	// the insert race for a brand-new key (E11000); the retry finds the
	// document and updates it.
	manifestSwapAttempts = 3
)

// bumpManifest points bump_manifests/<blockHash> at the current file.
type bumpManifest struct {
	BlockHash   string        `bson:"_id"`
	BlockHeight int64         `bson:"block_height"`
	FileID      bson.ObjectID `bson:"file_id"`
	Length      int64         `bson:"length"`
	UpdatedAt   time.Time     `bson:"updated_at"`
}

// stumpManifest points stump_manifests/<blockHash:subtreeIndex> at the
// current file.
type stumpManifest struct {
	Key          string        `bson:"_id"`
	BlockHash    string        `bson:"block_hash"`
	SubtreeIndex int64         `bson:"subtree_index"`
	FileID       bson.ObjectID `bson:"file_id"`
	Length       int64         `bson:"length"`
	UpdatedAt    time.Time     `bson:"updated_at"`
}

// blobRef is the (file id, length) pair a manifest resolves to.
type blobRef struct {
	FileID bson.ObjectID `bson:"file_id"`
	Length int64         `bson:"length"`
}

var projBlobRef = doc(kv(fFileID, 1), kv(fLength, 1))

func stumpKey(blockHash string, subtreeIndex int64) string {
	return fmt.Sprintf("%s:%d", blockHash, subtreeIndex)
}

// --- GridFS primitives ---

// firstWriteGate serializes the first upload to a bucket. The driver's
// GridFSBucket verifies its indexes on the first write and records that in
// an unsynchronized field, so two concurrent first uploads race on it —
// benign in effect (a duplicate createIndexes) but a data race all the same.
// Callers block only while the first upload is in flight; once it has
// succeeded every upload runs fully concurrently. A failed first upload
// leaves the gate armed so the next caller serializes again.
type firstWriteGate struct {
	mu   sync.Mutex
	done bool
}

func (g *firstWriteGate) run(f func() error) error {
	g.mu.Lock()
	if g.done {
		g.mu.Unlock()
		return f()
	}
	defer g.mu.Unlock()
	err := f()
	if err == nil {
		g.done = true
	}
	return err
}

// blobBucket pairs a GridFS bucket with its manifest collection and gate.
type blobBucket struct {
	bucket    *mongo.GridFSBucket
	manifests *mongo.Collection
	gate      firstWriteGate
}

// upload stores data as a new GridFS file under a fresh ObjectID. GridFS
// writes every chunk before the files document, so the file is complete by
// the time the id is handed to a manifest.
func (s *Store) upload(ctx context.Context, b *blobBucket, filename string, metadata bson.D, data []byte) (bson.ObjectID, error) {
	id := bson.NewObjectID()
	err := b.gate.run(func() error {
		us, err := b.bucket.OpenUploadStreamWithID(ctx, id, filename, options.GridFSUpload().SetMetadata(metadata))
		if err != nil {
			return fmt.Errorf("open upload: %w", err)
		}
		if _, err := us.Write(data); err != nil {
			_ = us.Abort()
			return fmt.Errorf("write chunks: %w", err)
		}
		if err := us.Close(); err != nil {
			_ = us.Abort()
			return fmt.Errorf("commit upload: %w", err)
		}
		return nil
	})
	return id, err
}

// download reads a whole GridFS file. A short read is an error, never
// partial data.
func (s *Store) download(ctx context.Context, bucket *mongo.GridFSBucket, ref blobRef) ([]byte, error) {
	if ref.Length < 0 {
		return nil, fmt.Errorf("file %s has negative length %d", ref.FileID.Hex(), ref.Length)
	}
	ds, err := bucket.OpenDownloadStream(ctx, ref.FileID)
	if err != nil {
		return nil, fmt.Errorf("open download: %w", err)
	}
	defer func() { _ = ds.Close() }()
	buf := make([]byte, ref.Length)
	if _, err := io.ReadFull(ds, buf); err != nil {
		return nil, fmt.Errorf("read %d bytes: %w", ref.Length, err)
	}
	return buf, nil
}

// deleteFile removes one GridFS file; one that is already gone is fine.
func (s *Store) deleteFile(ctx context.Context, bucket *mongo.GridFSBucket, id bson.ObjectID) error {
	if err := bucket.Delete(ctx, id); err != nil && !errors.Is(err, mongo.ErrFileNotFound) {
		return fmt.Errorf("delete file %s: %w", id.Hex(), err)
	}
	return nil
}

// --- manifest primitives ---

// swapManifest atomically points manifests/<key> at the fields in set and
// returns the previously referenced file, if the key existed. Two writers
// upserting a brand-new key can collide on _id; the loser retries and
// updates the document the winner inserted.
func (s *Store) swapManifest(ctx context.Context, manifests *mongo.Collection, key string, set bson.D) (prev blobRef, hadPrev bool, err error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	for attempt := 0; attempt < manifestSwapAttempts; attempt++ {
		err = manifests.FindOneAndUpdate(octx, idFilter(key), doc(kv(opSet, set)),
			options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.Before).SetProjection(projBlobRef)).Decode(&prev)
		switch {
		case err == nil:
			return prev, true, nil
		case errors.Is(err, mongo.ErrNoDocuments):
			return blobRef{}, false, nil // inserted
		case mongo.IsDuplicateKeyError(err):
			continue
		default:
			return blobRef{}, false, fmt.Errorf("swap manifest %s: %w", key, err)
		}
	}
	return blobRef{}, false, fmt.Errorf("swap manifest %s: %w", key, err)
}

// readRef resolves manifests/<key>, or store.ErrNotFound.
func (s *Store) readRef(ctx context.Context, manifests *mongo.Collection, key string) (blobRef, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var ref blobRef
	err := manifests.FindOne(octx, idFilter(key), options.FindOne().SetProjection(projBlobRef)).Decode(&ref)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ref, store.ErrNotFound
	}
	if err != nil {
		return ref, fmt.Errorf("read manifest %s: %w", key, err)
	}
	return ref, nil
}

// fetch downloads the file ref points at. If the download fails and the
// manifest now references a different file — a concurrent overwrite deleted
// ref between the caller's manifest read and this download — it follows the
// new reference once.
func (s *Store) fetch(ctx context.Context, bucket *mongo.GridFSBucket, manifests *mongo.Collection, key string, ref blobRef) ([]byte, error) {
	data, err := s.download(ctx, bucket, ref)
	if err == nil {
		return data, nil
	}
	fresh, rerr := s.readRef(ctx, manifests, key)
	if rerr != nil || fresh.FileID == ref.FileID {
		return nil, err
	}
	return s.download(ctx, bucket, fresh)
}

// replaceBlob is the shared write path: upload, swap the manifest, delete
// the file the swap replaced, sweep crash leftovers under scope.
func (s *Store) replaceBlob(ctx context.Context, b *blobBucket, key, filename string, metadata, manifestSet bson.D, data []byte) error {
	id, err := s.upload(ctx, b, filename, metadata, data)
	if err != nil {
		return err
	}
	set := append(doc(kv(fFileID, id), kv(fLength, int64(len(data))), kv(fUpdatedAt, msNow())), manifestSet...)
	prev, hadPrev, err := s.swapManifest(ctx, b.manifests, key, set)
	if err != nil {
		// Nothing references the upload; don't leave it behind.
		_ = s.deleteFile(ctx, b.bucket, id)
		return err
	}
	if hadPrev && prev.FileID != id {
		if err := s.deleteFile(ctx, b.bucket, prev.FileID); err != nil {
			return err
		}
	}
	s.sweepStale(ctx, b.bucket, metadata, id)
	return nil
}

// sweepStale deletes files under scope (metadata equality) that are older
// than staleUploadAge and are not keep: uploads whose writer died before the
// manifest swap. Best-effort — the write that called it has already landed.
func (s *Store) sweepStale(ctx context.Context, bucket *mongo.GridFSBucket, scope bson.D, keep bson.ObjectID) {
	cutoff := bson.NewObjectIDFromTimestamp(time.Now().Add(-staleUploadAge))
	filter := doc(kv(fID, doc(kv(opLt, cutoff), kv(opNe, keep))))
	for _, e := range scope {
		filter = append(filter, kv("metadata."+e.Key, e.Value))
	}
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := bucket.GetFilesCollection().Find(qctx, filter, options.Find().SetProjection(projID))
	if err != nil {
		return
	}
	var stale []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := cur.All(qctx, &stale); err != nil {
		return
	}
	for _, f := range stale {
		_ = s.deleteFile(ctx, bucket, f.ID)
	}
}

// --- compound BUMPs ---

// InsertBUMP implements store.Store. A rebuild for an existing block uploads
// the new compound, swaps the manifest, and deletes the superseded file, so
// GetBUMP never observes a gap; the parsed-BUMP cache is invalidated last.
func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	if blockHash == "" {
		return errors.New("insert bump: empty block hash")
	}
	h := heightToInt64(blockHeight)
	meta := doc(kv(metaBlockHash, blockHash), kv(metaBlockHeight, h))
	if err := s.replaceBlob(ctx, &s.bumps, blockHash, blockHash, meta, doc(kv(fBlockHeight, h)), bumpData); err != nil {
		return fmt.Errorf("insert bump %s: %w", blockHash, err)
	}
	s.bumpCache.Remove(blockHash)
	return nil
}

// GetBUMP implements store.Store, returning store.ErrNotFound for a block
// with no stored compound.
func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var m bumpManifest
	err := s.bumps.manifests.FindOne(octx, idFilter(blockHash)).Decode(&m)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil, store.ErrNotFound
	}
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	data, err := s.fetch(ctx, s.bumps.bucket, s.bumps.manifests, blockHash, blobRef{FileID: m.FileID, Length: m.Length})
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	return heightFromInt64(m.BlockHeight), data, nil
}

// DeleteBUMPByBlockHash implements store.Store; idempotent, and the cache
// entry is dropped whether or not a manifest existed. Only the referenced
// file is deleted, so an upload by a concurrent InsertBUMP that has not yet
// swapped its manifest survives and wins, as an insert after a delete should.
func (s *Store) DeleteBUMPByBlockHash(ctx context.Context, blockHash string) error {
	defer s.bumpCache.Remove(blockHash)
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var m bumpManifest
	err := s.bumps.manifests.FindOneAndDelete(octx, idFilter(blockHash)).Decode(&m)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete bump %s: %w", blockHash, err)
	}
	if err := s.deleteFile(ctx, s.bumps.bucket, m.FileID); err != nil {
		return fmt.Errorf("delete bump %s: %w", blockHash, err)
	}
	return nil
}

// EnrichMerklePath implements store.Store through the shared bumpcache, like
// every other backend: best-effort, no full-row read.
func (s *Store) EnrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	s.enrichMerklePath(ctx, status)
}

func (s *Store) enrichMerklePath(ctx context.Context, status *models.TransactionStatus) {
	if status == nil {
		return
	}
	s.bumpCache.Enrich(status, func() ([]byte, error) {
		_, data, err := s.GetBUMP(ctx, status.BlockHash)
		return data, err
	})
}

// enrichOrphanedProofs resolves each historical anchor's merkle path from the
// orphaned block's retained compound BUMP (issue #279). Best-effort.
func (s *Store) enrichOrphanedProofs(ctx context.Context, status *models.TransactionStatus) {
	if status == nil {
		return
	}
	for i := range status.OrphanedProofs {
		entry := &status.OrphanedProofs[i]
		if len(entry.MerklePath) > 0 || entry.BlockHash == "" {
			continue
		}
		hash := entry.BlockHash
		entry.MerklePath = s.bumpCache.MinimalPath(hash, status.TxID, func() ([]byte, error) {
			_, data, err := s.GetBUMP(ctx, hash)
			return data, err
		})
	}
}

// --- STUMPs ---

// InsertStump implements store.Store with upsert semantics per
// (block_hash, subtree_index).
func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	if stump == nil || stump.BlockHash == "" {
		return errors.New("insert stump: empty block hash")
	}
	idx := int64(stump.SubtreeIndex)
	key := stumpKey(stump.BlockHash, idx)
	meta := doc(kv(metaBlockHash, stump.BlockHash), kv(metaSubtreeIndex, idx))
	manifest := doc(kv(fBlockHash, stump.BlockHash), kv(fSubtreeIndex, idx))
	if err := s.replaceBlob(ctx, &s.stumps, key, key, meta, manifest, stump.StumpData); err != nil {
		return fmt.Errorf("insert stump %s/%d: %w", stump.BlockHash, stump.SubtreeIndex, err)
	}
	return nil
}

// GetStumpsByBlockHash implements store.Store, ordered by subtree index. The
// context is checked between downloads since each is a multi-round-trip read.
func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	manifests, err := s.stumpManifestsFor(ctx, blockHash)
	if err != nil {
		return nil, fmt.Errorf("get stumps %s: %w", blockHash, err)
	}
	out := make([]*models.Stump, 0, len(manifests))
	for _, m := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := s.fetch(ctx, s.stumps.bucket, s.stumps.manifests, m.Key, blobRef{FileID: m.FileID, Length: m.Length})
		if err != nil {
			return nil, fmt.Errorf("get stumps %s/%d: %w", blockHash, m.SubtreeIndex, err)
		}
		out = append(out, &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: int(m.SubtreeIndex),
			StumpData:    data,
		})
	}
	return out, nil
}

// DeleteStumpsByBlockHash implements store.Store. Each manifest is removed
// conditionally on the file it referenced at listing time, so a subtree
// concurrently re-inserted keeps its new file.
func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	manifests, err := s.stumpManifestsFor(ctx, blockHash)
	if err != nil {
		return fmt.Errorf("delete stumps %s: %w", blockHash, err)
	}
	for _, m := range manifests {
		if err := ctx.Err(); err != nil {
			return err
		}
		octx, cancel := s.opCtx(ctx)
		res, err := s.stumps.manifests.DeleteOne(octx, doc(kv(fID, m.Key), kv(fFileID, m.FileID)))
		cancel()
		if err != nil {
			return fmt.Errorf("delete stumps %s: %w", blockHash, err)
		}
		if res.DeletedCount == 0 {
			continue // replaced underneath; its writer owns the old file's deletion
		}
		if err := s.deleteFile(ctx, s.stumps.bucket, m.FileID); err != nil {
			return fmt.Errorf("delete stumps %s: %w", blockHash, err)
		}
	}
	return nil
}

// stumpManifestsFor lists a block's stump manifests ordered by subtree index.
func (s *Store) stumpManifestsFor(ctx context.Context, blockHash string) ([]stumpManifest, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := s.stumps.manifests.Find(qctx, doc(kv(fBlockHash, blockHash)), options.Find().SetSort(doc(kv(fSubtreeIndex, 1))))
	if err != nil {
		return nil, err
	}
	var out []stumpManifest
	if err := cur.All(qctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}
