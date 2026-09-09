package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/arcade/models"
	"github.com/bsv-blockchain/arcade/store"
)

// Metadata keys stored on GridFS files documents (queried as metadata.<key>).
const (
	metaBlockHash    = "block_hash"
	metaBlockHeight  = "block_height"
	metaSubtreeIndex = "subtree_index"
	metaField        = "metadata"
)

// gridfsFileMeta is the files-collection projection the blob reads decode.
type gridfsFileMeta struct {
	ID       bson.ObjectID `bson:"_id"`
	Length   int64         `bson:"length"`
	Metadata struct {
		BlockHash    string `bson:"block_hash"`
		BlockHeight  int64  `bson:"block_height"`
		SubtreeIndex int64  `bson:"subtree_index"`
	} `bson:"metadata"`
}

var projFileMeta = doc(kv(fID, 1), kv(fLength, 1), kv(metaField, 1))

// upload stores data as a new GridFS file under a fresh ObjectID. GridFS
// writes every chunk before the files document, so the file becomes visible
// atomically when the files document lands — the linearization point the
// other backends implement by hand.
func (s *Store) upload(ctx context.Context, bucket *mongo.GridFSBucket, filename string, metadata bson.D, data []byte) (bson.ObjectID, error) {
	id := bson.NewObjectID()
	us, err := bucket.OpenUploadStreamWithID(ctx, id, filename, options.GridFSUpload().SetMetadata(metadata))
	if err != nil {
		return id, fmt.Errorf("open upload: %w", err)
	}
	if _, err := us.Write(data); err != nil {
		_ = us.Abort()
		return id, fmt.Errorf("write chunks: %w", err)
	}
	if err := us.Close(); err != nil {
		_ = us.Abort()
		return id, fmt.Errorf("commit upload: %w", err)
	}
	return id, nil
}

// download reads a whole GridFS file. A short read is an error, never
// partial data.
func (s *Store) download(ctx context.Context, bucket *mongo.GridFSBucket, f gridfsFileMeta) ([]byte, error) {
	ds, err := bucket.OpenDownloadStream(ctx, f.ID)
	if err != nil {
		return nil, fmt.Errorf("open download: %w", err)
	}
	defer func() { _ = ds.Close() }()
	if f.Length < 0 {
		return nil, fmt.Errorf("file %s has negative length %d", f.ID.Hex(), f.Length)
	}
	buf := make([]byte, f.Length)
	if _, err := io.ReadFull(ds, buf); err != nil {
		return nil, fmt.Errorf("read %d bytes: %w", f.Length, err)
	}
	return buf, nil
}

// latestFile returns the newest files document matching filter, or
// store.ErrNotFound.
func (s *Store) latestFile(ctx context.Context, bucket *mongo.GridFSBucket, filter bson.D) (gridfsFileMeta, error) {
	octx, cancel := s.opCtx(ctx)
	defer cancel()
	var f gridfsFileMeta
	err := bucket.GetFilesCollection().FindOne(octx, filter,
		options.FindOne().SetSort(doc(kv(fUploadDate, -1))).SetProjection(projFileMeta)).Decode(&f)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return f, store.ErrNotFound
	}
	if err != nil {
		return f, fmt.Errorf("find file: %w", err)
	}
	return f, nil
}

// listFiles returns every files document matching filter in sort order.
func (s *Store) listFiles(ctx context.Context, bucket *mongo.GridFSBucket, filter, sort bson.D) ([]gridfsFileMeta, error) {
	qctx, cancel := s.queryCtx(ctx)
	defer cancel()
	opts := options.Find().SetProjection(projFileMeta)
	if len(sort) > 0 {
		opts.SetSort(sort)
	}
	cur, err := bucket.GetFilesCollection().Find(qctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("list files: %w", err)
	}
	var files []gridfsFileMeta
	if err := cur.All(qctx, &files); err != nil {
		return nil, fmt.Errorf("list files: %w", err)
	}
	return files, nil
}

// deleteFiles removes every file matching filter. GridFS deletes the files
// document first and the chunks second, so a concurrent reader either finds
// the whole file or none of it. A file that vanished underneath is fine.
func (s *Store) deleteFiles(ctx context.Context, bucket *mongo.GridFSBucket, filter bson.D) error {
	files, err := s.listFiles(ctx, bucket, filter, nil)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := bucket.Delete(ctx, f.ID); err != nil && !errors.Is(err, mongo.ErrFileNotFound) {
			return fmt.Errorf("delete file %s: %w", f.ID.Hex(), err)
		}
	}
	return nil
}

// --- compound BUMPs ---

// InsertBUMP implements store.Store. A rebuild for an existing block uploads
// the new compound first and deletes superseded copies after, so GetBUMP
// never observes a gap; the parsed-BUMP cache is invalidated last.
func (s *Store) InsertBUMP(ctx context.Context, blockHash string, blockHeight uint64, bumpData []byte) error {
	if blockHash == "" {
		return errors.New("insert bump: empty block hash")
	}
	meta := doc(kv(metaBlockHash, blockHash), kv(metaBlockHeight, heightToInt64(blockHeight)))
	id, err := s.upload(ctx, s.bumps, blockHash, meta, bumpData)
	if err != nil {
		return fmt.Errorf("insert bump %s: %w", blockHash, err)
	}
	if err := s.deleteFiles(ctx, s.bumps, doc(kv(fMetaBlockHash, blockHash), kv(fID, doc(kv(opNe, id))))); err != nil {
		return fmt.Errorf("insert bump %s: prune superseded: %w", blockHash, err)
	}
	s.bumpCache.Remove(blockHash)
	return nil
}

// GetBUMP implements store.Store, returning store.ErrNotFound for a block
// with no stored compound.
func (s *Store) GetBUMP(ctx context.Context, blockHash string) (uint64, []byte, error) {
	f, err := s.latestFile(ctx, s.bumps, doc(kv(fMetaBlockHash, blockHash)))
	if err != nil {
		return 0, nil, err
	}
	data, err := s.download(ctx, s.bumps, f)
	if err != nil {
		return 0, nil, fmt.Errorf("get bump %s: %w", blockHash, err)
	}
	return heightFromInt64(f.Metadata.BlockHeight), data, nil
}

// DeleteBUMPByBlockHash implements store.Store; idempotent, and the cache
// entry is dropped whether or not a file existed.
func (s *Store) DeleteBUMPByBlockHash(ctx context.Context, blockHash string) error {
	defer s.bumpCache.Remove(blockHash)
	if err := s.deleteFiles(ctx, s.bumps, doc(kv(fMetaBlockHash, blockHash))); err != nil {
		return fmt.Errorf("delete bump %s: %w", blockHash, err)
	}
	return nil
}

// blocksWithBUMP reports which of the given block hashes have a stored
// compound BUMP, in one query.
func (s *Store) blocksWithBUMP(ctx context.Context, hashes []string) (map[string]bool, error) {
	out := make(map[string]bool, len(hashes))
	if len(hashes) == 0 {
		return out, nil
	}
	files, err := s.listFiles(ctx, s.bumps, doc(kv(fMetaBlockHash, doc(kv(opIn, hashes)))), nil)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		out[f.Metadata.BlockHash] = true
	}
	return out, nil
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

func stumpFilename(blockHash string, subtreeIndex int) string {
	return fmt.Sprintf("%s:%d", blockHash, subtreeIndex)
}

// InsertStump implements store.Store with upsert semantics per
// (block_hash, subtree_index): upload, then prune older copies.
func (s *Store) InsertStump(ctx context.Context, stump *models.Stump) error {
	if stump == nil || stump.BlockHash == "" {
		return errors.New("insert stump: empty block hash")
	}
	idx := int64(stump.SubtreeIndex)
	meta := doc(kv(metaBlockHash, stump.BlockHash), kv(metaSubtreeIndex, idx))
	id, err := s.upload(ctx, s.stumps, stumpFilename(stump.BlockHash, stump.SubtreeIndex), meta, stump.StumpData)
	if err != nil {
		return fmt.Errorf("insert stump %s/%d: %w", stump.BlockHash, stump.SubtreeIndex, err)
	}
	filter := doc(kv(fMetaBlockHash, stump.BlockHash), kv(fMetaSubtreeIndex, idx), kv(fID, doc(kv(opNe, id))))
	if err := s.deleteFiles(ctx, s.stumps, filter); err != nil {
		return fmt.Errorf("insert stump %s/%d: prune superseded: %w", stump.BlockHash, stump.SubtreeIndex, err)
	}
	return nil
}

// GetStumpsByBlockHash implements store.Store, ordered by subtree index with
// the newest copy winning per subtree. The context is checked between
// downloads since each is a multi-round-trip read.
func (s *Store) GetStumpsByBlockHash(ctx context.Context, blockHash string) ([]*models.Stump, error) {
	files, err := s.listFiles(ctx, s.stumps, doc(kv(fMetaBlockHash, blockHash)),
		doc(kv(fMetaSubtreeIndex, 1), kv(fUploadDate, -1)))
	if err != nil {
		return nil, fmt.Errorf("get stumps %s: %w", blockHash, err)
	}
	out := make([]*models.Stump, 0, len(files))
	seen := make(map[int64]struct{}, len(files))
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, dup := seen[f.Metadata.SubtreeIndex]; dup {
			continue
		}
		seen[f.Metadata.SubtreeIndex] = struct{}{}
		data, err := s.download(ctx, s.stumps, f)
		if err != nil {
			return nil, fmt.Errorf("get stumps %s/%d: %w", blockHash, f.Metadata.SubtreeIndex, err)
		}
		out = append(out, &models.Stump{
			BlockHash:    blockHash,
			SubtreeIndex: int(f.Metadata.SubtreeIndex),
			StumpData:    data,
		})
	}
	return out, nil
}

// DeleteStumpsByBlockHash implements store.Store.
func (s *Store) DeleteStumpsByBlockHash(ctx context.Context, blockHash string) error {
	if err := s.deleteFiles(ctx, s.stumps, doc(kv(fMetaBlockHash, blockHash))); err != nil {
		return fmt.Errorf("delete stumps %s: %w", blockHash, err)
	}
	return nil
}
