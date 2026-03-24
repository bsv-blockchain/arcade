## 1. Core Implementation

- [x] 1.1 In `routes/fiber/callback.go` `handleStump`, add status update to MINED and event publishing before storing the STUMP data
- [x] 1.2 Ensure status update failure does not prevent STUMP storage (log warning, continue to InsertStump)

## 2. Testing

- [x] 2.1 Add test for `handleStump` verifying transaction status is updated to MINED when a valid STUMP callback is received
- [x] 2.2 Add test for `handleStump` verifying STUMP is still stored when status update fails
- [x] 2.3 Run existing tests to verify no regressions
