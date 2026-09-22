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
	"go.uber.org/zap"

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
// Every invariant in this file is stated against a primary. They are built out
// of read-then-write pairs, so a stale read breaks them: New warns about a
// non-primary read preference (it does not refuse one), and under that
// configuration none of what follows holds.
//
// A writer that dies between upload and swap leaves an unreferenced file, and
// so does one whose swap fails or reports an unknown outcome: the upload is
// kept on purpose there, because a swap that committed and then surfaced a
// network error would otherwise leave the manifest pointing at a file the
// writer deleted, which no later read could repair and no later write would
// notice. Unreferenced files are reclaimed instead: the next successful write
// for the same key sweeps files whose upload COMPLETED (files.uploadDate,
// stamped by the driver when the stream closes) more than staleUploadAge ago
// and that it does not reference. Age is taken from completion, not from the
// file id allocated before the upload began, so a slow in-flight upload can
// never look stale. Chunks are not covered: the driver writes them before the
// files document, so an upload whose Close AND whose Abort both fail leaves
// chunks with no files document for the sweep to find. That costs disk, never
// correctness — nothing can reference them.
//
// What makes "completed long ago and unreferenced" mean "its writer died" is
// swapWindow: a writer that took longer than that between its upload
// completing and its swap abandons the upload rather than publish it. No live
// writer can therefore swap in a file old enough to be swept, so the sweep
// cannot delete a file another writer is about to reference.
//
// That check runs on the writer's own clock, started as close to the server's
// uploadDate stamp as a client can get (inside upload, on the statement after
// Close). It cannot see a stall in the sliver before that sample, so it is
// backed by a check that needs no clock at all: after the swap lands the
// writer confirms the file it just published still exists, and republishes if
// it is gone while the manifest still names it. That last qualifier is the
// whole of it — a published file that is gone is usually not a problem at all
// but the ordinary result of losing a race, where a later writer swapped onto
// its own upload and deleted this one. Republishing there would undo a newer
// write, so who the manifest names now is what tells the two apart. Between
// them the two checks cover both directions — the window check keeps a
// sweepable file from being published, the confirmation catches a file that
// was swept before it was published.
//
// Two gaps remain, both narrow and both reported rather than hidden. The
// confirmation needs two reads to reach a verdict; when either cannot be
// completed it returns "not lost", because a failed read is not evidence of
// deletion, so a genuine loss during an outage is published as a success. And
// a republish that is swept again on every attempt exhausts
// blobPublishAttempts and returns an error with the manifest still naming the
// last, confirmed-gone file. In both cases the block's next write repairs the
// manifest, and until then its reads fail loudly rather than serving wrong
// bytes.

// Metadata keys stored on GridFS files documents (queried as metadata.<key>).
const (
	metaBlockHash    = "block_hash"
	metaBlockHeight  = "block_height"
	metaSubtreeIndex = "subtree_index"

	// staleUploadAge is how long an upload must have been complete and
	// unreferenced before a later write for the same key sweeps it as a
	// crash leftover.
	staleUploadAge = time.Hour

	// swapWindow bounds the gap between an upload completing and the
	// manifest swap that publishes it. Past it the writer abandons its own
	// upload, which is what lets sweepStale read "completed long ago and
	// unreferenced" as "the writer died" instead of "the writer is slow".
	// Far below staleUploadAge, so neither a descheduled goroutine nor skew
	// between the uploading pod's clock (the driver stamps uploadDate from
	// it) and a sweeping pod's clock can close the gap.
	swapWindow = 5 * time.Minute

	// manifestSwapAttempts bounds the retry of an upserting swap that lost
	// the insert race for a brand-new key (E11000); the retry finds the
	// document and updates it.
	manifestSwapAttempts = 3

	// blobPublishAttempts bounds the retry of a publish whose file was swept
	// between upload and swap. Reaching the bound needs that to happen on
	// every attempt, which means a host stalled for hours; the bound is here
	// so it fails loudly instead of spinning.
	blobPublishAttempts = 3
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

// blobRef is the (file id, length) pair a manifest resolves to, plus the
// block height a bump manifest carries (zero on stump manifests, which key on
// subtree instead). Height travels WITH the file reference so that a reader
// which follows the manifest to a replacement file also gets that manifest's
// height, never the one from the read it started with.
type blobRef struct {
	FileID      bson.ObjectID `bson:"file_id"`
	Length      int64         `bson:"length"`
	BlockHeight int64         `bson:"block_height"`
}

var projBlobRef = doc(kv(fFileID, 1), kv(fLength, 1), kv(fBlockHeight, 1))

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
//
// It also returns when the upload completed, sampled on the statement after
// Close returns — Close is where the driver stamps files.uploadDate (from this
// process's clock, not the server's), the field sweepStale ages files by, so
// this is the closest a caller can read
// that stamp without fetching it back. The caller measures its swapWindow
// from here rather than from its own later statements, so nothing between
// the two can be silently excluded from the window.
func (s *Store) upload(ctx context.Context, b *blobBucket, filename string, metadata bson.D, data []byte) (bson.ObjectID, time.Time, error) {
	id := bson.NewObjectID()
	var completed time.Time
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
		completed = time.Now()
		return nil
	})
	return id, completed, err
}

// fileMissing reports whether a GridFS file is definitively gone. A query
// that cannot be completed reports an error, never absence: not knowing is
// not the same as knowing it was deleted.
func (s *Store) fileMissing(ctx context.Context, bucket *mongo.GridFSBucket, id bson.ObjectID) (bool, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	err := bucket.GetFilesCollection().FindOne(octx, doc(kv(fID, id)), options.FindOne().SetProjection(projID)).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return true, nil
	}
	return false, err
}

// download reads a whole GridFS file. A short read is an error, never partial
// data.
//
// The manifest's length is checked against files.length rather than trusted on
// its own: a manifest length larger than the file would otherwise size an
// allocation off a corrupt or half-restored document, and one smaller would
// read a prefix and return it as the whole compound — a truncated BUMP that
// parses is worse than an error.
//
// This is a cross-check between two documents, not a bound derived from the
// bytes actually stored: files.length is counted by the driver as it writes
// and the server never validates it against the chunks. Corruption confined to
// one of the two documents is caught; a restore that rewrote both consistently
// would still be believed.
func (s *Store) download(ctx context.Context, bucket *mongo.GridFSBucket, ref blobRef) ([]byte, error) {
	if ref.Length < 0 {
		return nil, fmt.Errorf("file %s has negative length %d", ref.FileID.Hex(), ref.Length)
	}
	ds, err := bucket.OpenDownloadStream(ctx, ref.FileID)
	if err != nil {
		return nil, fmt.Errorf("open download: %w", err)
	}
	defer func() { _ = ds.Close() }()
	stored := ds.GetFile().Length
	if stored != ref.Length {
		return nil, fmt.Errorf("file %s is %d bytes but its manifest claims %d", ref.FileID.Hex(), stored, ref.Length)
	}
	buf := make([]byte, stored)
	if _, err := io.ReadFull(ds, buf); err != nil {
		return nil, fmt.Errorf("read %d bytes: %w", stored, err)
	}
	return buf, nil
}

// deleteFile removes one GridFS file; one that is already gone is fine.
func (s *Store) deleteFile(ctx context.Context, bucket *mongo.GridFSBucket, id bson.ObjectID) error {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	if err := bucket.Delete(octx, id); err != nil && !errors.Is(err, mongo.ErrFileNotFound) {
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
	for attempt := 0; attempt < manifestSwapAttempts; attempt++ {
		// A fresh budget per attempt, for the reason in withDupKeyRetry: the
		// attempts that retry here are the ones that lost a race, so a shared
		// deadline is spent by the attempts least able to afford it.
		octx, cancel := s.opCtx(ctx)
		err = manifests.FindOneAndUpdate(octx, idFilter(key), doc(kv(opSet, set)),
			options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.Before).SetProjection(projBlobRef)).Decode(&prev)
		cancel()
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
//
// It returns the reference the bytes were actually served from, so a caller
// that reports manifest metadata alongside the payload reports the metadata
// of the manifest that named the file, not of a manifest a rebuild has since
// replaced.
func (s *Store) fetch(ctx context.Context, bucket *mongo.GridFSBucket, manifests *mongo.Collection, key string, ref blobRef) ([]byte, blobRef, error) {
	data, err := s.download(ctx, bucket, ref)
	if err == nil {
		return data, ref, nil
	}
	fresh, rerr := s.readRef(ctx, manifests, key)
	if rerr != nil || fresh.FileID == ref.FileID {
		return nil, ref, err
	}
	data, err = s.download(ctx, bucket, fresh)
	return data, fresh, err
}

// replaceBlob is the shared write path. publishBlob does the work; this
// retries the one outcome that is worth retrying, a file swept out from
// under the writer between its upload and its swap.
func (s *Store) replaceBlob(ctx context.Context, b *blobBucket, key, filename string, metadata, manifestSet bson.D, data []byte) error {
	for attempt := 0; attempt < blobPublishAttempts; attempt++ {
		published, err := s.publishBlob(ctx, b, key, filename, metadata, manifestSet, data)
		if err != nil {
			return err
		}
		if published {
			return nil
		}
	}
	return fmt.Errorf("blob %s: upload swept before it could be published, %d attempts running", key, blobPublishAttempts)
}

// publishBlob uploads data, swaps the manifest onto it, deletes the file the
// swap replaced and sweeps crash leftovers under scope. It reports whether
// the file it published is still there; false means the manifest is
// momentarily pointing at a swept file and the caller should publish again.
func (s *Store) publishBlob(ctx context.Context, b *blobBucket, key, filename string, metadata, manifestSet bson.D, data []byte) (bool, error) {
	id, uploaded, err := s.upload(ctx, b, filename, metadata, data)
	if err != nil {
		return false, err
	}
	set := append(doc(kv(fFileID, id), kv(fLength, int64(len(data))), kv(fUpdatedAt, msNow())), manifestSet...)
	// Almost always zero — the swap is the next statement — but not free:
	// a writer descheduled or frozen in between would otherwise publish a
	// file sweepStale is by then entitled to delete. Measured from inside
	// upload, right after the Close that stamps uploadDate.
	if waited := time.Since(uploaded); waited > swapWindow {
		if derr := s.deleteFile(ctx, b.bucket, id); derr != nil {
			s.logger.Warn("abandoned upload could not be deleted; the stale sweep will reclaim it", zap.String("key", key), zap.Error(derr))
		}
		return false, fmt.Errorf("blob %s: waited %s between upload and manifest swap, over the %s window", key, waited, swapWindow)
	}
	prev, hadPrev, err := s.swapManifest(ctx, b.manifests, key, set)
	if err != nil {
		// Keep the upload. The swap's outcome is unknown on error — a
		// findAndModify that commits can still surface a network failure —
		// and deleting a file the manifest now references is unrecoverable,
		// while an upload nothing references is swept later.
		return false, err
	}
	// The swap has landed, so everything below is best-effort: the new file
	// is in force whatever happens, and a copy that survives a transient
	// delete failure is unreferenced and swept by a later write. Deleting
	// what this writer replaced happens first and unconditionally — it is
	// this writer's file to remove no matter what the checks below decide,
	// and skipping it is how a second copy survives a concurrent overwrite.
	if hadPrev && prev.FileID != id {
		if derr := s.deleteFile(ctx, b.bucket, prev.FileID); derr != nil {
			// Not fatal — the new file is in force — but not silent either:
			// a delete that keeps failing is how chunks pile up unnoticed.
			s.logger.Warn("superseded blob file could not be deleted; the stale sweep will reclaim it", zap.String("key", key), zap.Error(derr))
		}
	}
	// One thing below the swap is worth a round trip. The swapWindow check
	// runs on this host's clock, so a stall between the driver stamping
	// uploadDate inside Close and the sample taken just after it is
	// invisible to the check,
	// and in that sliver a sweeper could have taken this upload for a crash
	// leftover — leaving the manifest pointing at nothing, which no read
	// repairs and no write notices. publishedFileLost detects exactly that,
	// and only that; republish when it does.
	if s.publishedFileLost(ctx, b, key, id) {
		return false, nil
	}
	s.sweepStale(ctx, b.bucket, metadata, id)
	return true, nil
}

// publishedFileLost reports the one state that needs republishing: the file
// this writer published is gone while the manifest still names it.
//
// "Gone" on its own is the ordinary outcome of losing a race — a later writer
// swapped the manifest onto its own upload and deleted the file it replaced,
// which is this one. That writer's data is in force and republishing over it
// would undo a newer write, so who the manifest names now is what separates
// the two. A check that cannot be completed is not evidence of anything, and
// returns false.
//
// The verdict is two reads, not one atomic observation, and both must reach a
// primary to mean anything. New only WARNS about a non-primary read
// preference rather than refusing it, so that is a configuration this code can
// actually meet: against a lagging secondary this returns false for a file
// that is genuinely gone, the republish never fires, and the manifest is left
// naming a deleted file for good. A writer
// that swaps in between them can still have its file deleted by the republish
// that follows — the same last-swap-wins outcome as any other concurrent
// overwrite, since the republish swaps later, but worth knowing it is not
// excluded.
func (s *Store) publishedFileLost(ctx context.Context, b *blobBucket, key string, id bson.ObjectID) bool {
	gone, err := s.fileMissing(ctx, b.bucket, id)
	if err != nil || !gone {
		return false
	}
	cur, err := s.readRef(ctx, b.manifests, key)
	return err == nil && cur.FileID == id
}

// sweepStale deletes files under scope (metadata equality) whose upload
// completed more than staleUploadAge ago and that are not keep: uploads
// whose writer died before the manifest swap. Best-effort — the write that
// called it has already landed — but not silent: a sweep that keeps failing
// is how chunks accumulate until the disk fills, so every failure is logged.
func (s *Store) sweepStale(ctx context.Context, bucket *mongo.GridFSBucket, scope bson.D, keep bson.ObjectID) {
	cutoff := time.Now().Add(-staleUploadAge)
	filter := doc(kv(fUploadDate, doc(kv(opLt, cutoff))), kv(fID, doc(kv(opNe, keep))))
	for _, e := range scope {
		filter = append(filter, kv("metadata."+e.Key, e.Value))
	}
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	cur, err := bucket.GetFilesCollection().Find(qctx, filter, options.Find().SetProjection(projID))
	if err != nil {
		s.logger.Warn("stale upload sweep could not list files", zap.Any("scope", scope), zap.Error(err))
		return
	}
	var stale []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := cur.All(qctx, &stale); err != nil {
		s.logger.Warn("stale upload sweep could not read the file list", zap.Any("scope", scope), zap.Error(err))
		return
	}
	for _, f := range stale {
		if derr := s.deleteFile(ctx, bucket, f.ID); derr != nil {
			s.logger.Warn("stale upload could not be deleted", zap.String("file_id", f.ID.Hex()), zap.Error(derr))
		}
	}
}

// --- compound BUMPs ---

// InsertBUMP implements store.Store. A rebuild for an existing block uploads
// the new compound, swaps the manifest, and deletes the superseded file, so
// GetBUMP never observes a gap. The parsed-BUMP cache is invalidated on every
// exit, success or not: an error after the swap must not leave the cache
// serving the compound the manifest no longer points at.
func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	if blockHash == "" {
		return errors.New("insert bump: empty block hash")
	}
	defer s.bumpCache.Remove(blockHash)
	h := heightToInt64(blockHeight)
	meta := doc(kv(metaBlockHash, blockHash), kv(metaBlockHeight, h))
	if err := s.replaceBlob(ctx, &s.bumps, blockHash, blockHash, meta, doc(kv(fBlockHeight, h)), bumpData); err != nil {
		return fmt.Errorf("insert bump %s: %w", blockHash, err)
	}
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
	data, served, err := s.fetch(ctx, s.bumps.bucket, s.bumps.manifests, blockHash,
		blobRef{FileID: m.FileID, Length: m.Length, BlockHeight: m.BlockHeight})
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	// served, not m: if fetch followed the manifest to a replacement file,
	// the height must be the replacement's too.
	return heightFromInt64(served.BlockHeight), data, nil
}

// DeleteBUMPByBlockHash implements store.Store; idempotent, and the cache
// entry is dropped whether or not a manifest existed. The manifest delete is
// unconditional, so a concurrent InsertBUMP for the same block resolves by
// server order — last writer wins — the same way the single-statement DELETE
// does on Postgres and Pebble.
//
// Every interleaving still leaves consistent state, because findAndModify
// hands back the manifest it removed and only that manifest's file is
// deleted: a swap that lands first is removed together with the file it
// published (the delete won), and a swap that lands after re-creates the
// manifest over an upload this call never saw and cannot touch (the insert
// won). Neither order can leave a manifest pointing at a deleted file.
//
// DeleteStumpsByBlockHash deletes conditionally on the file it listed rather
// than unconditionally, for reasons that do not apply here: it walks many
// manifests one at a time, so it is not a single atomic statement, and it
// runs during reorg cleanup while the builder may be re-inserting the same
// subtrees. This is one document, and the reorg reconciler deliberately
// never deletes BUMPs (issue #279) — it is an operator lever.
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

// GetStumpsByBlockHash implements store.Store, ordered by subtree index.
//
// The downloads run with bounded parallelism: each is a manifest read plus a
// GridFS files read plus a chunks cursor, so a block with thousands of
// subtrees spent thousands of round trips strictly one after another — with
// the reconciler waiting on all of them. Results land in their own slot, so
// the subtree order the caller is promised comes from the manifest listing
// and not from completion order. forEach stops dispatching at the first
// failure and checks ctx before each call, which is what the serial loop's
// per-iteration ctx.Err() did.
func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	manifests, err := s.stumpManifestsFor(ctx, blockHash)
	if err != nil {
		return nil, fmt.Errorf("get stumps %s: %w", blockHash, err)
	}
	out := make([]*models.Stump, len(manifests))
	if err := forEach(ctx, len(manifests), s.rowConcurrency, func(i int) error {
		m := manifests[i]
		// The subtree index is part of the key, so nothing a replacement
		// could change is reported here; the served reference is not needed.
		data, _, ferr := s.fetch(ctx, s.stumps.bucket, s.stumps.manifests, m.Key, blobRef{FileID: m.FileID, Length: m.Length})
		if ferr != nil {
			return fmt.Errorf("get stumps %s/%d: %w", blockHash, m.SubtreeIndex, ferr)
		}
		out[i] = &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: int(m.SubtreeIndex),
			StumpData:    data,
		}
		return nil
	}); err != nil {
		return nil, err
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
	// Three round trips per subtree (conditional manifest delete, then the
	// bucket's files delete and chunks delete), so a reorged block with
	// thousands of subtrees is thousands of serial round trips inside reorg
	// cleanup. Each subtree is independent — the conditional delete is what
	// makes it safe, not the ordering — so they run with the same bounded
	// parallelism as the rewrites.
	return forEach(ctx, len(manifests), s.rowConcurrency, func(i int) error {
		m := manifests[i]
		octx, cancel := s.opCtx(ctx)
		res, err := s.stumps.manifests.DeleteOne(octx, doc(kv(fID, m.Key), kv(fFileID, m.FileID)))
		cancel()
		if err != nil {
			return fmt.Errorf("delete stumps %s: %w", blockHash, err)
		}
		if res.DeletedCount == 0 {
			return nil // replaced underneath; its writer owns the old file's deletion
		}
		if err := s.deleteFile(ctx, s.stumps.bucket, m.FileID); err != nil {
			return fmt.Errorf("delete stumps %s: %w", blockHash, err)
		}
		return nil
	})
}

// stumpManifestsFor lists a block's stump manifests ordered by subtree index.
// The caller's context, not queryCtx: the result is one manifest per subtree
// with no limit, so it grows with the block, and both callers
// (GetStumpsByBlockHash, DeleteStumpsByBlockHash) then do work per row. A
// fixed 8 s deadline on a data-sized read fails a large block's rebuild or
// reorg cleanup every time it is attempted, which is the failure mode the
// GetTxIDsByBlockHash and CensusStatusesSince changes already removed.
func (s *Store) stumpManifestsFor(ctx context.Context, blockHash string) ([]stumpManifest, error) {
	cur, err := s.stumps.manifests.Find(ctx, doc(kv(fBlockHash, blockHash)), options.Find().SetSort(doc(kv(fSubtreeIndex, 1))))
	if err != nil {
		return nil, err
	}
	var out []stumpManifest
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}
