## 1. Single Transaction Broadcast

- [x] 1.1 Modify `SubmitTransaction` in `service/embedded/embedded.go` to wait for ALL endpoint goroutines instead of returning on first result
- [x] 1.2 Add status aggregation logic: collect all results, return best status (ACCEPTED_BY_NETWORK > SENT_TO_NETWORK > REJECTED), falling back to timeout status if none complete

## 2. Batch Transaction Broadcast

- [x] 2.1 Modify `SubmitTransactions` in `service/embedded/embedded.go` to wait for ALL endpoint goroutines instead of returning on first result
- [x] 2.2 Apply same best-status-wins aggregation for batch results (any endpoint success = batch accepted)

## 3. Testing

- [x] 3.1 Add test for single-tx broadcast hitting all endpoints (verify all endpoints called, best status returned)
- [x] 3.2 Add test for batch broadcast hitting all endpoints
- [x] 3.3 Add test for mixed results (one accept, one reject) returning the favorable status
- [x] 3.4 Verify existing `fetchBlockDataForBUMP` tests still pass (no changes to fetch behavior)
