package store

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/arcade/models"
)

// forEachBlockProcessingMaxPage bounds the adaptive page growth in
// ForEachBlockProcessing. 64k rows at one height is far beyond any real
// competition; hitting the cap returns an error instead of silently
// skipping rows.
const forEachBlockProcessingMaxPage = 1 << 16

// ForEachBlockProcessing streams every block_processing row with
// block_height >= minHeight through fn, newest first, using
// ListBlockProcessingStatus pages.
//
// ListBlockProcessingStatus paginates with a `block_height < before` keyset
// cursor over a NON-unique key: naively advancing the cursor to the last
// row's height skips any rows at that same height beyond the page boundary
// — precisely the same-height competitors a reorg scan exists to find
// (issue #279). This helper is boundary-safe: it re-includes the boundary
// height on the next page (`before = last+1`) and, when an entire full page
// sits at one height, grows the page until the height fits. The trade-off
// is that fn can see a boundary row MORE THAN ONCE — callers must be
// idempotent per row (both reorg scans are: they compare against the active
// chain and collect into sets).
//
// fn returning a non-nil error stops the iteration and surfaces the error.
func ForEachBlockProcessing(ctx context.Context, s Store, minHeight uint64, pageSize int, fn func(*models.BlockProcessingStatus) error) error {
	if pageSize <= 0 {
		return fmt.Errorf("pageSize must be > 0")
	}
	before := uint64(0) // 0 = unbounded (newest first)
	limit := pageSize
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := s.ListBlockProcessingStatus(ctx, before, limit)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			// Heights are descending: everything after the first row below
			// minHeight is below it too.
			if row.BlockHeight < minHeight {
				return nil
			}
			if err := fn(row); err != nil {
				return err
			}
		}
		if len(rows) < limit {
			return nil // final page
		}
		last := rows[len(rows)-1].BlockHeight
		if rows[0].BlockHeight == last {
			// The whole page sits at one height; the cursor cannot advance
			// without skipping unseen same-height rows. Grow the page.
			if limit >= forEachBlockProcessingMaxPage {
				return fmt.Errorf("block_processing height %d has more than %d rows; scan incomplete", last, limit)
			}
			limit *= 2
			continue
		}
		if last == 0 {
			return nil
		}
		// Re-include the boundary height: rows above it are fully covered
		// (descending order), rows AT it may extend past the page end.
		before = last + 1
	}
}
