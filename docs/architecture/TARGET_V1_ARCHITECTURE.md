# Target V1 Architecture

This document captures the intended end-state architecture for Legato after the
architecture reset. It supersedes the older framing that treated Legato as a
fixed-block cache in front of a remote library.

## Product Definition

Legato is a read-only, distributed sample-library filesystem specialized for
DAW and sampler workloads.

The important design point is that Legato is not just a generic cache plus a
mount shim. It owns:

- the semantic transfer unit for library content
- the client-side residency model
- the replication and invalidation contract
- the native mounted filesystem behavior seen by DAWs

Legato still runs in user space on client machines. It does not require a
bespoke kernel driver or a custom disk filesystem implementation below the host
OS filesystem. The filesystem intelligence lives in the Legato runtime and in
the on-disk store format it manages.

## Architectural Position

Legato should be built as an extent-oriented distributed filesystem with
log-structured stores on both server and client.

That means:

- the server owns a Legato-managed catalog and extent store over host-backed
  storage on the TrueNAS side
- the client owns a Legato-managed local extent store and residency catalog on
  NVMe
- the mount layer exposes normal read-only filesystem semantics to DAWs
- the wire protocol moves semantic extents and change records, not arbitrary
  fixed blocks plus server-local file handles

This is intentionally not a general-purpose distributed filesystem. It is
specialized for:

- many small metadata-heavy instrument files
- large sequentially streamed sample content
- rare but real container/random-access cases
- project-aware warming before latency-sensitive playback

## Core Principles

### Per-File Transfer Layout

Legato does not use one global block size.

At ingest time, the server classifies files into layout classes and records a
transfer layout for each file. The starting model is:

- `UNITARY`
  Small files fetched and cached as one extent.
- `STREAMED`
  Sequentially consumed files stored as larger extents with aggressive
  readahead and prefetch support.
- `RANDOM`
  Container-style or uncertain files stored as smaller extents with no
  optimistic readahead.

The exact thresholds are implementation policy, not protocol constants. They
should be tunable and overrideable by path or vendor-specific rules.

The classification exists because the sample-library workload is not uniform:

- small instrument and metadata files are usually read whole
- large sample payloads are usually consumed sequentially
- random-access container cases are the minority but still need explicit
  support

The protocol and local store should reflect those facts directly.

### Legato-Managed Stores

Legato owns its own extent store and catalog format, but uses the host
filesystem as the durable storage substrate.

The server-side store lives on top of host-backed storage on the TrueNAS side.
The client-side store lives on top of the client host filesystem on NVMe.

Legato is therefore more filesystem-native than a SQLite-plus-blobs cache, but
less operationally extreme than building a fully custom block allocator and disk
filesystem for each platform.

This is an intentional narrowing of scope:

- more coherent than a generic cache shim
- much less extreme than building a bespoke kernel or raw-disk filesystem

### Read Path Optimized For Playback

The read path should be designed around how samplers actually consume data.

Implications:

- small instrument/config files should not pay fixed-block amplification
- sequential sample content should be fetched in larger extents than small-file
  metadata
- cold first-touch should fetch the head extent in the foreground and continue
  warming in the background
- the mount layer should serve cached reads locally without requiring a server
  round-trip on the hot path

The system should optimize for "first bytes quickly, then continue warming"
rather than "one generic cache strategy for every file."

### Residency Is First-Class State

The client store tracks which extents are resident locally as part of the
filesystem state, not as an incidental side table glued onto opaque blobs.

This state drives:

- mount read behavior
- explicit prefetch completion semantics
- pinning and eviction
- restart recovery
- observability

## On-Disk Model

The target store shape is log-structured.

The store contains:

- immutable extent segments
- a catalog mapping logical file identity to current metadata and extent layout
- a client residency map for local materialization state
- checkpoints and compaction state

The store should support:

- append-only writes for newly fetched or ingested extents
- recovery by replay from a recent checkpoint
- integrity verification per extent
- compaction and eviction without rewriting the whole store

The client and server stores should therefore converge on the same semantic
model even if their operational roles differ.

This does not require the repository to implement a full custom disk
filesystem. It does require replacing the current "SQLite metadata plus opaque
block files" model with a coherent Legato-owned store format.

## Logical Objects

The primary logical objects are:

- `File`
  Stable logical identity for one library file.
- `InodeMetadata`
  Path, size, mtime, type, and layout metadata for a file or directory.
- `Extent`
  One semantic byte range of a file stored in the server or client store.
- `ResidencyEntry`
  Whether a client has a specific extent locally present and valid.
- `ChangeRecord`
  Ordered mutation record for metadata or content changes emitted by the server.

## Protocol Shape

Legato should retain gRPC over TLS 1.3 with mutual TLS unless measurement proves
it inadequate. The transport can stay; the protocol semantics should change.

The target request families are:

- `Resolve`
  Resolve a path to stable file identity plus current inode/layout metadata.
- `Fetch`
  Request one or more extents by logical reference and stream back extent
  records.
- `Subscribe`
  Subscribe to ordered server change records from a durable sequence cursor.
- `Hint`
  Submit prioritized prefetch intent in terms of extents or logical file
  regions.
- `CatalogSync`
  Optional bounded catch-up or recovery request for clients that need catalog
  changes since a known checkpoint.

The protocol should stop depending on:

- server-local open handles as a core correctness mechanism
- one global fixed block size
- block alignment as the primary semantic unit

The protocol should also stop treating server-local open handles as the main
anchor for correctness. Stable logical identity plus layout metadata should be
the primary contract.

## Server Runtime Shape

The server remains a single Rust service running in a container on the TrueNAS
side and managed through Komodo.

Its responsibilities become:

- ingesting and classifying library content into the Legato store
- maintaining the catalog
- serving resolve/fetch/subscribe operations
- emitting ordered change records
- reconciling host-side library changes into store updates

The canonical content still lives on the TrueNAS side. Legato owns the
materialized metadata and extent layout derived from it.

## Client Runtime Shape

The client remains a native Rust binary with platform-specific mount adapters on
macOS and Windows.

Its responsibilities become:

- mounting the library as a read-only native filesystem
- maintaining the local extent store and residency catalog
- resolving paths to local metadata
- serving cache hits directly from the local store
- fetching missing extents from the server
- subscribing to ordered server changes and invalidating local state
- exposing residency-aware prefetch and pinning semantics

## Prefetch Model

`legato-prefetch` remains a separate binary, but it should speak the native
Legato storage model rather than a synthetic fixed-block cache model.

It should:

- parse project and sampler inputs
- resolve referenced files
- map them to file layouts and extents
- submit prioritized hint/fetch plans
- optionally wait until required extents are resident locally

Prefetch success means client-local residency for the requested priority set,
not merely "the server accepted a warm-up request."

## Mount Behavior

The mounted filesystem remains read-only.

The user-visible behavior is:

- normal directory and file traversal from DAWs and plugins
- low-latency access to cached content
- predictable cold-miss behavior driven by extent fetch and first-touch warming
- invalidation of stale local metadata after upstream changes

Legato does not need to be POSIX-complete. It does need to be reliable for the
read-only operations that samplers and DAWs actually use.

## Security And Trust

The security model remains:

- mTLS between clients and server
- authenticated clients allowed to read the library
- no fine-grained ACL system in v1

Integrity should be enforced at the extent level. Content hashes are for
verification and recovery, not necessarily the primary addressing scheme.

## Deliberate Non-Goals

The target architecture intentionally excludes:

- a bespoke kernel filesystem implementation
- a custom disk filesystem below the host filesystem
- general-purpose writable distributed filesystem semantics
- multi-writer coordination for library mutation
- shared cache coherence between clients beyond ordered server change streams
- a requirement that the current SQLite block cache model be preserved
- content-addressed deduplication as the primary storage design center

## Repository Consequences

This architecture reset implies that some current implementation work is
transitional rather than foundational.

Likely to survive in some form:

- Rust workspace structure
- gRPC transport and mTLS bootstrap
- native mount integration shape
- project-analysis and prefetch parsing work
- config, telemetry, packaging, deployment, and CI scaffolding

Likely to be replaced or heavily rewritten:

- fixed-block cache model
- open-handle-centric read protocol
- client cache schema
- server metadata and file-serving assumptions
- invalidation semantics tied to the current metadata/block design

## Success Criteria

The architecture reset is complete when:

- the wire protocol is extent- and change-record-oriented
- the client serves reads from a Legato-managed local store rather than a simple
  fixed-block cache
- prefetch operates on semantic transfer layouts
- server and client recovery semantics are defined by the new store model
- mount behavior, packaging, deployment, and tests all reflect the new design
  rather than the provisional one
