# Plan 001: Bound expired-job maintenance performed by each claim

> Drift check: `git diff --stat 02e6509..HEAD -- crates/lenso-jobs-plugin/src/storage.rs crates/lenso-jobs-plugin/migrations crates/lenso-jobs-plugin/tests`.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `02e6509`, 2026-08-30

## Why this matters

Every single-item claim first updates all expired exhausted jobs. A large expired
backlog therefore makes every worker scan and lock far more rows than the work claimed.

## Current state

- `src/storage.rs:116-127` retires all matching rows without a limit.
- `src/storage.rs:129-143` then claims exactly one row with `SKIP LOCKED`.
- The claim index orders by availability but not the expired-lease maintenance path.

## Scope

In scope: jobs storage, additive indexes/migrations, and PostgreSQL backlog tests.
Out of scope: retry semantics, lease fencing tokens, and public Capability shapes.

## Steps

1. Add tests with a large expired/exhausted set and multiple workers, asserting bounded
   rows retired per transaction and no job loss/duplicate claim.
2. Retire a configurable or internal fixed batch through a CTE selecting rows
   `FOR UPDATE SKIP LOCKED LIMIT n`, then claim one due job.
3. Add an index led by queue/status/lease expiry as justified by `EXPLAIN` in the test
   fixture; preserve ordering for normal queued claims.

## Verification

- `lenso-cargo test -p lenso-jobs-plugin --include-ignored` -> all pass with PostgreSQL.
- `lenso-cargo check -p lenso-jobs-plugin --all-targets` -> exit 0.
- `git diff --check` -> no output.

## STOP conditions

Stop if bounded retirement can strand an eligible retry behind exhausted rows; adjust
the query design before implementation rather than increasing the batch unboundedly.
