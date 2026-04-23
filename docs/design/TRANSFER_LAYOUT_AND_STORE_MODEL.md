# Transfer Layout And Store Model

This document defines the Legato store and transfer model.

## Workload Shape

Legato is optimized for sample libraries with a bimodal shape:

- many small instrument, preset, map, and metadata files
- many large audio sample files consumed sequentially
- a smaller set of container-like files with scattered reads

The storage model is semantic. File layout is chosen from the file's expected access pattern rather than from a global byte size.

## Transfer Classes

Files are classified during ingest.

### `UNITARY`

Small files are represented as one extent.

Typical examples:

- instrument definitions
- preset files
- zone maps
- small metadata assets

### `STREAMED`

Sequentially consumed sample content is represented as larger ordered extents.

Typical examples:

- medium and large sample files
- sustained sample payloads
- files where first-touch reads start near offset zero and continue forward

The read path fetches the required extent and schedules nearby following extents when useful.

### `RANDOM`

Container-like or uncertain files are represented as smaller conservative extents.

Typical examples:

- monolithic sample containers
- index or database-like assets inside vendor libraries
- files whose useful access pattern is scattered

## Classification Inputs

Classification uses:

- extension
- magic bytes
- file size
- directory context
- known sampler or vendor structure
- explicit path-pattern override rules

Overrides exist for deterministic handling of unusual libraries.

## Extent Addressing

Legato addresses content with logical extent references.

An extent map entry contains:

- file offset
- length
- segment identifier
- segment offset
- payload hash
- transfer class

Records are self-validating through hashes. File identity and extent map position are the normal operational keys.

## Segment Store

Legato storage is append-only segment files plus catalog/checkpoint files.

### Segment Files

Segments are ordinary host-filesystem files. A segment contains:

- segment header
- ordered records
- segment footer
- intra-segment index

Records include:

- `EXTENT`
  File data payload plus extent metadata.
- `INODE`
  File metadata and extent map reference.
- `DIRENT`
  Directory membership.
- `TOMBSTONE`
  Logical deletion.
- `CHECKPOINT`
  Durable recovery boundary.

Each record carries:

- record type
- monotonic sequence number
- payload length
- payload hash
- payload bytes

Sealed segments are immutable.

### Catalog

The catalog maps:

- path to file ID
- file ID to active inode record
- directory ID to active directory record
- subscription cursor to replay position

Catalog updates are append-only and compacted periodically into checkpointed catalog files. Startup loads the latest catalog checkpoint and replays later records.

### Client Residency

Client residency is part of local filesystem state.

A client inode records:

- the authoritative extent map
- which extents are locally resident
- local segment locations for resident extents
- pin state
- last-access and priority metadata for eviction

Reads of absent extents enqueue fetch work and wait until the required data is resident or the read fails.

## Compaction And Eviction

Segments are reclaimed by compaction.

Compaction:

- selects segments with low retained utility
- rewrites useful resident records into new segments
- advances catalog references
- drops obsolete sealed segments

Client eviction uses:

- pin state
- prefetch priority
- last access time
- segment utility
- configured maximum local store size

Pinned project dependencies are not evicted while the pin remains active.

## Checkpoint And Recovery

Recovery is bounded by checkpoints.

Startup sequence:

1. Load the latest catalog checkpoint.
2. Replay segment and catalog records after the checkpoint sequence.
3. Validate record hashes.
4. Truncate incomplete tail records.
5. Rebuild in-memory indexes.
6. Resume server subscription from the durable cursor.

Unclean shutdown leaves at most an incomplete tail record and uncheckpointed replay work.

## Server Store

The server store is canonical.

It contains:

- complete catalog
- complete inode set
- complete directory set
- authoritative extent records
- ordered record stream

The server can import from an existing directory tree. Import writes Legato records into the canonical store using the classification rules above.

## Client Store

The client store is a partial replica.

It contains:

- catalog records needed for mounted paths
- resident extent records
- local residency metadata
- checkpoint state
- client subscription cursor

The client may know about a file without having all of its extents locally resident.

## Mount Layer

The mount layer is user-space-native:

- macOS through the macOS adapter surface
- Windows through WinFSP

The mount layer translates filesystem callbacks into catalog lookup, extent-map lookup, residency checks, local segment reads, and remote fetches.

## Design Boundaries

Legato does not implement:

- a raw storage-device filesystem
- a kernel filesystem as the product surface
- byte-level deduplication as the primary storage objective
- writable library mutation through the mounted client filesystem

## Implementation Consequences

Code should align around these primitives:

- record
- segment
- catalog
- inode
- directory entry
- extent map
- residency
- checkpoint
- compaction
- subscription cursor
