# Performance And Readiness Issue Track

This document captures the main implementation gaps found during a full inventory of the current system against the intended Legato design.

The goal here is not feature expansion. It is to identify the code work required for Legato to actually outperform direct SMB/NFS access for local DAW workflows where the storage machine and host machine are different.

## Current Assessment

Legato is no longer a stub. The current repo contains:

- a real server ingest path into a Legato-managed segment/catalog store
- gRPC + mTLS transport
- a local client partial store
- a real read path that resolves metadata, fetches missing extents, and serves reads from local resident data
- macOS and Windows mount adapters
- a project-aware prefetch CLI
- packaging and CI for server, macOS client, and Windows client

The architecture is directionally correct for DAW use.

The current implementation is not yet likely to be meaningfully better than SMB/NFS in practice, because several core performance and correctness properties are still too weak.

## High-Priority Issues

### 1. Reconcile rewrites file extents before proving the file changed

Current behavior:

- server reconcile appends extent payload records during file processing
- only after writing extents does it compare the resulting inode to the existing inode

Why this matters:

- unchanged libraries can be rewritten into fresh segments during startup or rescan
- startup cost scales badly with library size
- canonical store growth becomes noisy and wasteful
- this directly undermines the value of a persistent server-side canonical store

Primary code:

- `crates/legato-server/src/canonical.rs`

Required fix:

- compare current file identity and layout inputs before writing extents
- reuse existing extent metadata when content is unchanged
- only append new extent payloads when the file actually changed

### 2. Extent reads scan segment files instead of using a direct index

Current behavior:

- local and server extent reads reopen a segment and scan records to find the matching payload

Why this matters:

- warm reads pay unnecessary CPU and IO overhead
- larger segment counts will degrade read latency
- this is exactly the wrong trade for playback-sensitive DAW reads

Primary code:

- `crates/legato-client-cache/src/catalog.rs`
- `crates/legato-client-cache/src/segment.rs`

Required fix:

- persist and load a real segment index
- resolve `(segment_id, segment_offset)` directly to payload location
- keep hash verification, but stop doing linear scan on the hot read path

### 3. Client catalog and residency are collapsed together too aggressively

Current behavior:

- client inode recording drops authoritative extent-map knowledge unless extents are locally resident
- the client mostly behaves like a resident-extent tracker plus remote resolve path

Why this matters:

- partial replica semantics are weaker than the design docs claim
- offline behavior is limited
- the client cannot cleanly distinguish "known remotely" from "resident locally"
- eviction and replay are harder to reason about

Primary code:

- `crates/legato-client-cache/src/client_store.rs`

Required fix:

- store authoritative inode metadata and full extent maps durably
- track residency separately from canonical extent-map knowledge
- let the client know a file layout without requiring local payload presence

### 4. Prefetch does not go through a real coordinated residency control plane

Current behavior:

- server `Hint` is only an acknowledgment surface
- `legato-prefetch` warms data by opening and reading paths through its own runtime path

Why this matters:

- prefetch is not coordinated with the mounted runtime as a first-class mechanism
- there is duplicated runtime logic
- future concurrency and residency guarantees become messy
- wait-through semantics are weaker than the design language suggests

Primary code:

- `crates/legato-server/src/rpc.rs`
- `crates/legato-prefetch/src/lib.rs`

Required fix:

- either implement real coordinated hint handling end to end
- or make `legato-prefetch` talk to the already-running local mount/runtime through an explicit local control API
- avoid multiple independent writers mutating the same client store without coordination

### 5. Cache limits are not enforced on the actual read-through path

Current behavior:

- fetched extents are stored locally
- checkpointing happens
- `max_cache_bytes` is not enforced during ordinary read-through
- compaction and eviction are minimal maintenance commands, not an active policy

Why this matters:

- local store growth can exceed intended limits
- long-running DAW usage can drift into uncontrolled state growth
- the current "cache" behavior is not operationally tight enough

Primary code:

- `crates/legato-client-core/src/filesystem.rs`
- `crates/legato-client-cache/src/client_store.rs`

Required fix:

- enforce cache budget during normal fetch/store operations
- maintain enough metadata to make eviction decisions meaningful
- make compaction and eviction part of steady-state behavior, not just manual repair

### 6. Eviction policy is simpler than the design and likely too blunt for real use

Current behavior:

- eviction mostly removes extents by reverse file offset order until under the limit

Why this matters:

- it ignores the design intent around pinning, priority, last access, and utility
- it can discard useful data too aggressively
- it will reduce the chance that repeated project loads stay warm

Primary code:

- `crates/legato-client-cache/src/client_store.rs`

Required fix:

- add real residency metadata
- support pinning for active project dependencies
- incorporate access recency and priority into eviction scoring

### 7. No clear single-writer or shared-store coordination model

Current behavior:

- `legatofs` and `legato-prefetch` can both operate on the same state directory
- there is no obvious store lock or coordination protocol

Why this matters:

- concurrent local mutation is a correctness risk
- corruption risk is higher under exactly the workflows that matter most
- this becomes more likely once prefetch is used habitually before DAW launch

Primary code:

- `crates/legatofs/src/main.rs`
- `crates/legato-prefetch/src/lib.rs`
- `crates/legato-client-cache`

Required fix:

- define and implement single-writer ownership or safe shared access
- if the mounted runtime owns the store, prefetch should request work through it rather than mutating state directly

### 8. Store layout docs and runtime layout are not fully aligned

Current behavior:

- docs describe meaningful `catalog/` and `tmp/` usage
- runtime primarily uses `segments/` and `checkpoints/`
- `catalog/` is created but not substantively used

Why this matters:

- design drift makes future work easier to mis-implement
- operations docs imply behaviors that are not actually present

Primary code:

- `crates/legato-client-cache/src/catalog.rs`
- `deploy/OPERATIONS.md`
- `docs/architecture/SYSTEM_SHAPE.md`

Required fix:

- either make runtime layout match the documented shape
- or simplify the docs to match the actual persisted layout

### 9. Stale operational defaults remain in the TrueNAS helper script

Current behavior:

- the helper script still defaults to UID/GID `10001`
- compose defaults to a different runtime identity

Why this matters:

- creates avoidable deployment failures
- increases the chance of permission problems on the server datasets

Primary code:

- `deploy/truenas/create-legato-datasets.sh`
- `compose.yaml`

Required fix:

- align defaults with the actual expected deployment identity
- remove stale values so operations match runtime assumptions

## DAW-Specific Judgment

For your use case, Legato only wins if it reliably turns playback-time reads into local NVMe reads with low overhead and predictable warm-cache behavior.

That means the important question is not "does it mount" or "does it fetch." The important question is:

- does the second and third project load become materially better than SMB/NFS
- does playback avoid network sensitivity once warmed
- does the system avoid doing obviously expensive extra work on startup and read paths

Right now:

- the architecture can beat SMB/NFS
- the implementation is not consistently there yet

The two biggest reasons are:

1. warm reads are still more expensive than they should be because segment lookup is too naive
2. steady-state store behavior is not tight enough yet because reconcile, eviction, and local coordination are still too rough

## What Must Be True Before Legato Is Actually Better

Legato becomes meaningfully better for DAW work when these conditions are met:

- unchanged libraries do not get re-ingested into fresh segments on restart or rescan
- resident extent reads resolve directly, without linear segment scanning
- prefetch and mount runtime share one coherent residency model
- the client store can retain full file layout metadata even when only some extents are resident
- cache growth and eviction behave predictably under repeated project-open workflows
- local state mutation is coordinated so prefetch and mount activity cannot fight each other

## Verification Snapshot

At the time this issue track was written:

- local `make ci` passed
- the latest GitHub `CI/CD` run for `main` passed
- macOS and Windows packaging jobs both passed

That means the repo currently builds and tests successfully.

It does not mean the runtime is already at the performance or operational quality level needed to clearly beat direct network shares for DAW use.
