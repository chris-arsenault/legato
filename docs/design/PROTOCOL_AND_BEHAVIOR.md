# Protocol And Behavior

This document captures the final behavioral shape of Legato as a project-aware, read-only sample-library system. It focuses on the contract the system is designed to provide and the intentional boundaries around that contract.

## Scope

Legato serves a large sample library from the TrueNAS side to macOS and Windows DAW clients, with project-aware prefetch designed to make playback-time reads hit local NVMe.

In scope:

- read-only library access
- native filesystem mounting on client machines
- metadata resolution and extent fetches
- local caching
- project-aware prefetch

Not in scope:

- write support for the library
- DAW project collaboration or project-file sync
- generalized shared-filesystem semantics

## Goals

Legato is intended to provide these user-visible behaviors:

- the mounted library appears as a normal local directory to DAWs and plugins
- playback-time reads come from the local cache after successful prefetch
- cold project open is dominated by a bounded prefetch transfer instead of thousands of latency-sensitive small reads
- the server remains the canonical source of truth and clients remain caches

## System Behavior

Legato consists of:

1. a server daemon on the TrueNAS side
2. a native user-space filesystem client
3. a project-aware prefetch tool

The server indexes the library, serves metadata and semantic extents, and emits invalidations. The client mounts the library, maintains the local extent store, serves filesystem reads, and retries through reconnect or stale-handle cases. The prefetch tool analyzes project or plugin state and asks the local client runtime to warm the cache.

## Metadata And File Identity

The server maintains a persistent metadata index in SQLite.

The important behavioral rules are:

- file identity is stable across normal daemon restarts
- the metadata index is reconciled, not blindly rebuilt
- library changes produce invalidation events that clients use to drop stale metadata or cache entries

File IDs are logical identifiers owned by the server index, not direct inode aliases.

## Extent Model

Legato addresses data in semantic extents:

- extents are keyed by file identity plus extent index
- extent size is derived from the server-side layout decision
- streamed files can be prefetched ahead of playback-sensitive reads

The client extent store persists fetched extents locally and verifies integrity before use.

## Wire Behavior

Legato uses gRPC over TLS 1.3 with mTLS.

The important request families are:

- attach/session negotiation
- stat, path resolution, and directory listing
- resolve and semantic extent fetch
- explicit prefetch submission
- invalidation subscription

The key behavioral choices are:

- path resolution exists independently of file open
- prefetch is client-orchestrated and means client-cache residency, not just server warming
- invalidations refresh correctness after library changes

## Prefetch Behavior

`legato-prefetch` analyzes project inputs and emits prioritized work for the local client runtime.

Current input families are:

- Ableton `.als`
- Kontakt `.nki`
- plugin state blobs such as `.fxp`, `.fxb`, and `.vstpreset`

Priority levels are intentionally coarse:

- `P0`
  Direct sample references needed immediately.
- `P1`
  Files needed to instantiate the project or plugin state.
- `P2`
  Referenced sample content that is likely needed soon after load.
- `P3`
  Speculative or readahead work.

If the caller asks to wait through a priority, completion means those accepted ranges are durably resident in the client cache before the prefetch command returns.

## Client Behavior

The client runtime includes:

- a metadata cache for path and directory lookups
- an extent cache on local disk
- fetch coordination so overlapping requests share work
- prefetch scheduling and residency tracking
- reconnect and stale-handle recovery

The mounted filesystem is read-only. Unsupported mutating operations fail as read-only rather than pretending to succeed.

## Consistency Model

The consistency model is intentionally simple:

- the server is the source of truth
- clients are caches
- invalidations tell clients when to discard stale state
- reconnect logic restores session state after transport loss

Legato does not attempt cross-client cache coherence beyond the server invalidation stream. Each client cache is independent.

## Failure Behavior

The system is designed to degrade in predictable ways:

- if the server is unavailable and the data is already cached, playback can continue from cache
- if a cache miss occurs during a partition, the read waits until timeout and then fails
- if a cached extent fails integrity verification, it is evicted and refetched
- if transport state becomes stale after restart, the client reconnects and retries
- if project parsing cannot produce a precise prefetch set, the system falls back to a broader but safe prefetch shape

The important design rule is that failure should degrade to slower behavior or over-fetching, not silent corruption.

## Observability

The system emits structured tracing and Prometheus-style metrics.

The intended visibility includes:

- cache hit and miss behavior
- extent-fetch latency
- invalidation activity
- prefetch queue or residency behavior
- bytes served from cache versus network

## Deliberate What-Nots

The following are intentionally outside the design:

- a write path for library mutation
- a POSIX-complete filesystem target
- a custom bespoke transport protocol
- shared external control of cache residency
- multi-node or clustered metadata ownership

## Explicit Trade-Offs

- Read-only semantics over general filesystem behavior:
  Chosen because the target workload is sample playback, not collaborative file mutation.
- Coarse priority classes over highly dynamic scheduling models:
  Chosen because they are easier to reason about and integrate into launcher behavior.
- Client-owned residency guarantees over server-owned warming semantics:
  Chosen so the place that serves reads is also the place that proves readiness.
- Safe fallback prefetch over parser-perfect precision:
  Chosen because over-fetching is preferable to a project opening with missing data.
