# Transfer Layout And Store Model

This document captures the forward-looking transfer and storage rules for the
v1 architecture reset.

It is intentionally narrow:

- what Legato should build
- what it should not build
- why those choices fit the sample-library workload

## Workload Shape

Legato is optimized for a library shape that is both metadata-heavy and
stream-heavy.

The important workload facts are:

- many instrument and preset files are small and typically read whole
- many sample payloads are large and typically consumed sequentially
- truly random-access container cases exist, but they are the minority

That means a single global fixed block size is the wrong primary abstraction.
It over-fetches small metadata files and creates too much indexing overhead for
large sequential sample content.

## Transfer Classes

Legato should classify files into transfer/layout classes at ingest time and
persist that layout as part of file metadata.

The baseline classes are:

- `UNITARY`
  Small files transferred and cached as a single extent.
- `STREAMED`
  Sequentially consumed files split into larger extents with readahead-friendly
  behavior.
- `RANDOM`
  Container-like or uncertain files split into smaller extents with conservative
  fetch behavior.

These classes are policy, not protocol constants. Thresholds and heuristics
should be configurable and overrideable.

## Classification Inputs

Classification should use signals such as:

- extension and magic bytes
- file size
- directory context
- sampler/library-specific structure where known
- explicit override rules for vendor or path patterns

Legato should support an override mechanism for edge cases where heuristics are
not good enough. The override mechanism exists to make the intended layout
deterministic for unusual vendor libraries, not to preserve multiple competing
design directions.

## Head-Biased First-Touch

Cold reads for `STREAMED` content should be head-biased.

The required behavior is:

- foreground fetch of the first extent on first uncached access
- background warming of the following extents

This is important because samplers often need the beginning of a file quickly
and then continue consuming the remainder sequentially. The system should be
optimized for "play now, keep warming" rather than "wait for a large generic
block plan."

## Extent Addressing

Logical addressing should be extent-oriented rather than fixed-block-oriented.

Legato should identify content through:

- stable logical file identity
- file layout metadata
- logical extent references

Content hashes belong in the design for:

- integrity verification
- recovery
- corruption detection

They should not be treated as the primary design center for addressing. This is
not a deduplication-first system.

## Store Model

The target server and client stores are log-structured Legato-managed stores on
top of host filesystems.

The important design choice is:

- Legato owns the semantic storage model
- the host filesystem provides durable storage substrate

This avoids two bad extremes:

- a SQLite metadata database plus opaque generic blob files with weak semantic
  coupling
- a bespoke kernel or raw-disk filesystem implementation that would explode
  complexity and platform burden

The store should support:

- append-only extent writes
- checkpoints for bounded recovery
- compaction
- eviction on the client
- residency tracking on the client
- ordered change propagation from the server

## Mount Layer Position

The mount layer remains user-space-native on macOS and Windows.

Legato should become smarter in user space, not deeper in the kernel.

That means:

- the mount runtime should understand file layouts and extent residency
- it should not depend on a bespoke kernel filesystem implementation
- it should not attempt to become a general-purpose writable filesystem

## Intentional What-Nots

Legato should not build:

- a full custom disk filesystem below the host filesystem
- a bespoke kernel driver as the primary product direction
- a fixed-block cache model as the long-term design center
- a generalized distributed filesystem with full writable POSIX semantics
- a design whose main goal is content-addressed deduplication

## Consequence For Implementation

The system should converge toward:

- resolve to stable logical file identity plus layout
- fetch semantic extents
- subscribe to ordered logical changes
- prefetch by file layout and residency requirements
- mount from local catalog and extent residency state

Anything that still assumes "open remote file handle, fetch generic block,
track residency in a side table" should be treated as provisional.
