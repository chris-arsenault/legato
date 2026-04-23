# Protocol And Behavior

This document captures the behavioral contract for Legato as a read-only, project-aware sample-library filesystem.

## Scope

Legato serves a large sample library from a TrueNAS-side canonical store to macOS and Windows DAW clients. Clients mount a local filesystem view backed by partial local residency.

In scope:

- read-only library access
- native filesystem mounting on client machines
- catalog resolution
- semantic extent fetches
- ordered record subscription
- local residency and eviction
- project-aware prefetch

Outside the product boundary:

- write support through the mounted library
- DAW project collaboration or project-file sync
- generalized shared-filesystem mutation semantics

## Goals

Legato provides these user-visible behaviors:

- the mounted library appears as a normal local directory to DAWs and plugins
- cached playback-time reads come from local NVMe
- cold project open is dominated by bounded prefetch and extent fetch work
- the server is authoritative
- each client is a partial local replica

## System Behavior

Legato consists of:

1. a server daemon on the TrueNAS side
2. a native user-space filesystem client
3. project-aware prefetch logic invoked by the client when supported project files are opened

The server owns the canonical store and streams filesystem records. The client mounts a local read-only view, stores local segment records, tracks residency, and fetches misses. When the mounted client sees a supported project or preset open, it analyzes that file and asks the local runtime to make specific extents resident.

## File Identity And Catalog

Legato identifies files with stable logical file IDs.

Catalog records define:

- file ID
- path
- inode metadata
- directory entries
- layout class
- extent map
- modification sequence

Directory listings and file stat behavior are satisfied from catalog records. Path resolution produces the active inode record for a path.

## Extent Model

Legato addresses data in semantic extents:

- extents are keyed by file identity plus extent map entry
- extent size is derived from the file layout decision
- extent records carry payload hashes for integrity
- streamed files can be prefetched ahead of playback-sensitive reads

Content hashes verify records. They are not the primary identity of a file.

## Wire Behavior

Legato uses gRPC over TLS 1.3 with mTLS.

Core request families:

- `Attach`
  Establish protocol version and capability negotiation.
- `Resolve`
  Resolve a path to file ID, inode metadata, and extent map.
- `Fetch`
  Stream requested extent records.
- `Hint`
  Submit prioritized residency work.
- `Subscribe`
  Stream ordered records after a sequence cursor.

The hot path uses file identity and extent references. Path resolution happens before extent fetch.

## Client Read Behavior

Read path:

1. Platform adapter receives a read for a mounted file.
2. Client maps the path to an inode from the local catalog.
3. Client maps the requested byte range to one or more extent records.
4. Resident extents are read from local segments.
5. Missing extents are fetched, appended to local segments, marked resident, and then read.

Directory listing and stat calls are served from catalog state.

## Prefetch Behavior

When the mounted client opens a supported project or preset, `legato-prefetch` analyzes that input and emits prioritized residency work. The same logic is also exposed as an optional CLI, but manual requests are forwarded to the mounted client runtime instead of writing local residency state directly.

Input families:

- Ableton `.als`
- Kontakt `.nki`
- plugin state files such as `.fxp`, `.fxb`, and `.vstpreset`

Priority levels:

- `P0`
  Direct sample references needed immediately.
- `P1`
  Files needed to instantiate the project or plugin state.
- `P2`
  Referenced sample content likely needed soon after load.
- `P3`
  Speculative or readahead work.

If the caller uses the optional CLI and waits through a priority, completion means accepted extents at or above that priority are locally resident before the command returns.

## Consistency Model

The server's record stream is totally ordered by sequence number.

Client behavior:

- replay records in order
- update local catalog records
- preserve resident extents that still match active inode maps
- drop residency for obsolete extent mappings
- resume subscription from the last durable cursor

Each client owns its own residency state. Cross-client residency coordination is not part of the protocol.

## Failure Behavior

The system degrades predictably:

- resident data can be served while disconnected
- a miss during disconnection waits until timeout and then fails
- corrupt records are rejected by hash verification
- unclean shutdown recovery replays from the last checkpoint
- prefetch parser uncertainty results in broader safe residency requests

Failure should degrade to slower reads or broader fetching, not silent corruption.

## Observability

The system emits structured tracing and Prometheus-style metrics.

Useful visibility includes:

- extent hit and miss behavior
- extent-fetch latency
- record subscription lag
- catalog replay duration
- segment compaction and eviction activity
- prefetch residency progress

## Deliberate Boundaries

Legato does not implement:

- a write path for library mutation through the mounted filesystem
- general collaborative filesystem semantics
- shared external control of client residency
- multi-server catalog ownership

## Explicit Trade-Offs

- Client-owned residency guarantees:
  The client that serves reads is the component that proves readiness.
- Coarse priority classes:
  Four priorities are enough for project-open and playback-sensitive scheduling.
- Record replay:
  Ordered records make reconnect, catalog refresh, and partial replication one mechanism.
- Read-only mount:
  The useful workflow is fast, predictable sample-library reads.
