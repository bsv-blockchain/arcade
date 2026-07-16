## MODIFIED Requirements

### Requirement: Grace window is conditional on expected-STUMP-set completeness

The BUMP builder SHALL read the stored STUMPs for a block BEFORE waiting the configured grace window (`bump_builder.grace_window_ms`), and SHALL wait the window only when completeness of the STUMP set cannot be verified from that first read. When merkle-service's expected-STUMP set (`expectedSubtreeIndices` on BLOCK_PROCESSED) is fully satisfied by the stored STUMPs, the builder SHALL build the compound BUMP immediately.

#### Scenario: Expected set complete on arrival

- **WHEN** a BLOCK_PROCESSED message arrives with `expectedSubtreeIndices: [0, 1]` and STUMPs for subtrees 0 and 1 are already stored
- **THEN** the builder SHALL skip the grace window, build the compound BUMP from the stored STUMPs, stamp `processed_at`, and record the build under outcome `finalized_complete_no_grace`.

#### Scenario: Expected set incomplete — wait, then defer

- **WHEN** a BLOCK_PROCESSED message arrives with `expectedSubtreeIndices: [0, 1]` and only subtree 0's STUMP is stored, and the missing STUMP has not arrived by the end of the grace window
- **THEN** the builder SHALL wait the full grace window, re-read the STUMPs, and — the set still being incomplete — leave the block un-finalized (no BUMP, no `processed_at`) so the watchdog re-drives it via `/reprocess`, recording the message under outcome `deferred_incomplete`.

#### Scenario: Expected set incomplete — late STUMP lands within the window

- **WHEN** a BLOCK_PROCESSED message arrives with `expectedSubtreeIndices: [0]`, no STUMP stored, and subtree 0's STUMP is stored while the grace window is running
- **THEN** the builder SHALL find the completed set on the post-window re-read, build the compound BUMP, and stamp `processed_at`, recording the build under outcome `grace_waited`.

#### Scenario: Expected set absent, zero STUMPs — finalize without waiting

- **WHEN** a BLOCK_PROCESSED message arrives with no `expectedSubtreeIndices` field and no STUMPs stored for the block
- **THEN** the builder SHALL finalize the block immediately as an empty block (stamp `processed_at`, no BUMP, no grace-window wait), because an absent expected set means the block has no tracked transactions (merkle-service ≥ v0.4.5 emits the field whenever STUMPs are expected).

#### Scenario: Expected set absent, STUMPs present — completeness unverifiable

- **WHEN** a BLOCK_PROCESSED message arrives with no `expectedSubtreeIndices` field but one or more STUMPs are stored for the block
- **THEN** the builder SHALL log a warning that completeness cannot be verified, wait the full grace window, re-read the STUMPs, and build from the post-window set, recording the build under outcome `grace_waited`.

### Requirement: Build-outcome labels distinguish the grace dispositions

The BUMP builder SHALL stamp every terminal disposition of a BLOCK_PROCESSED message onto `arcade_bump_builder_build_duration_seconds{outcome}` using a closed, pre-registered label set in which successful builds carry `finalized_complete_no_grace` (grace window skipped after completeness verification) or `grace_waited` (all other successful builds), and completeness deferrals carry `deferred_incomplete`. The labels `success` and `incomplete_stumps` SHALL NOT be emitted.

#### Scenario: Deferral is stamped with the renamed failure label

- **WHEN** a block is left un-finalized because expected STUMPs are still missing after the grace window
- **THEN** the duration observation carries `outcome="deferred_incomplete"` and the `outcome="incomplete_stumps"` series is not emitted.

#### Scenario: Pre-registration exports the new labels from the first scrape

- **WHEN** the bump-builder service constructs and pre-registers its metrics
- **THEN** child series for `finalized_complete_no_grace`, `grace_waited`, and `deferred_incomplete` exist at zero before any message is handled.
