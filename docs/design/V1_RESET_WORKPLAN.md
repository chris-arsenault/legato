# V1 Reset Workplan

This document translates the target architecture into implementation tracks.

It is intentionally release-oriented. It does not preserve the old MVP framing.

## Track 1: Protocol Reset

Replace the current open/stat/block protocol with an extent-oriented protocol.

Deliverables:

- new protobuf messages for resolve, fetch, hint, subscribe, and durable cursors
- removal or deprecation plan for server-local handle semantics
- wire-compatibility/versioning rules for the reset

## Track 2: Server Catalog And Extent Store

Build the server-side Legato store.

Deliverables:

- ingest pipeline that classifies files and writes extent layouts
- server catalog with stable file identity and layout metadata
- extent fetch service over the new protocol
- ordered change-record stream
- override support for vendor/path-specific layout rules where heuristics are
  insufficient

## Track 3: Client Store And Residency

Replace the current block cache with a Legato-managed local extent store.

Deliverables:

- local extent segments
- residency catalog
- integrity verification
- restart recovery and checkpointing
- pinning, eviction, and compaction

## Track 4: Native Mount Runtime

Rework the mount adapters around the new store and protocol.

Deliverables:

- path resolution from local catalog
- extent-aware cold read behavior
- head-biased first-touch fetch
- invalidation-driven metadata refresh
- removal of assumptions tied to current open-handle behavior
- extent-residency-aware read behavior rather than generic fixed-block warming

## Track 5: Prefetch Rework

Move prefetch from fixed-block warming to semantic extent planning.

Deliverables:

- file-layout-aware prefetch plans
- policy for `UNITARY` / `STREAMED` / `RANDOM`
- residency-based wait semantics
- vendor/path override support for classification policy

## Track 6: Operations And Release

Make the new shape operable and releasable.

Deliverables:

- updated server dataset/state layout
- updated client install/bootstrap flow
- observability for extent fetch, residency, pinning, and compaction
- realistic end-to-end tests for cold reads, prefetch, restart, and server-side
  library updates

## Intentional What-Nots

This workplan does not include:

- building a custom kernel driver
- building a custom disk filesystem below the host filesystem
- preserving the current cache schema for compatibility
- pretending the old protocol is the long-term contract
- broadening scope into a general-purpose writable filesystem
