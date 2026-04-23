# Protocol Hardening

This is a point-in-time protocol note for Legato.

Its purpose is to lock down the release-shape protocol semantics around identity, replay, invalidation, and fetch correctness. The current implementation is usable enough to evaluate, but it is not yet a sufficiently hardened protocol for release.

## Intended Protocol Shape

Legato should behave as a read-only distributed sample-library protocol with these properties:

- clients see a logical virtual library namespace
- the server owns stable file identity
- path lookup resolves to stable file identity plus a versioned inode/layout snapshot
- data fetches are bound to that resolved inode/layout version
- ordered change replay is the authoritative coherence mechanism
- invalidations are a latency optimization, not the correctness mechanism
- prefetch is coordinated through the local runtime that owns residency

## Hardening Decisions

### 1. Wire paths are logical, not server-local absolute paths

The protocol namespace must not expose `/srv/libraries/...` or any other server-local mount path.

The wire-visible library should be rooted logically, for example:

- `/Kontakt/piano.nki`
- `/Spitfire/Strings/long.ncw`

The server may import from `/srv/libraries`, but that is deployment state, not protocol state.

## 2. `file_id` is stable and not derived from path

`file_id` must be server-owned stable identity.

Required semantics:

- rename keeps `file_id`
- content update in place keeps `file_id`
- inode/layout version changes on update
- delete removes the path mapping and tombstones the file identity
- delete and recreate at the same path yields a new `file_id`

The protocol must not treat path as identity.

## 3. `Resolve` returns a versioned inode snapshot

`Resolve` is the authoritative metadata operation for regular files.

It should return:

- `file_id`
- logical path
- inode generation
- file size
- mtime
- transfer class
- full extent map
- content metadata sufficient for stale detection

The returned inode/layout snapshot is what later `Fetch` calls are bound to.

## 4. `Fetch` is generation-bound

`Fetch` must not implicitly read "whatever the current inode says now."

Each fetch request must be tied to the inode snapshot the client resolved earlier.

Required behavior:

- client sends `file_id`
- client sends `inode_generation`
- client sends extent identity within that inode version
- server returns extent payloads only if the requested inode generation is still valid
- stale generation returns `FAILED_PRECONDITION` and forces re-resolve

This prevents mixed-generation reads across concurrent library updates.

## 5. Ordered change replay is authoritative

The protocol needs one authoritative coherence path.

That path is the ordered change stream.

Required behavior:

- every catalog mutation is assigned a globally ordered sequence
- clients persist the last durable replay cursor
- reconnect resumes replay from cursor
- replay is sufficient to rebuild coherent client metadata state
- clients remain correct even if invalidation delivery is delayed or dropped

`SubscribeChanges` should be a live ordered stream starting after a cursor, not just a finite dump helper.

## 6. Change application uses explicit batch commit boundaries

A reconcile pass can touch many paths.

Clients should not be forced to guess when a coherent batch has finished.

Required behavior:

- related records are emitted as one ordered batch
- the batch has an explicit commit boundary
- clients may buffer and then atomically expose batch results at commit

This avoids transient mixed views during replay.

## 7. Invalidation is best-effort only

Invalidation is still useful, but it is not the source of truth.

Required semantics:

- invalidation can tell the client to drop fast-path cached views immediately
- invalidation loss must not break correctness
- reconnect can safely begin with a coarse root invalidation
- subtree invalidation means recursive invalidation of all descendant paths

The client must still rely on ordered replay for durable convergence.

## 8. `Hint` is not part of the release server protocol until it is real

The current wire-level `Hint` method does not provide real residency semantics.

Release decision:

- server protocol does not advertise `Hint` capability until it is implemented end to end
- project-aware prefetch should talk to the local mounted runtime through a local control API
- the component that serves reads owns residency guarantees

This keeps residency coordination local and avoids lying in capability negotiation.

## 9. Capability negotiation must be exact

`Attach` should not return a fixed capability set regardless of the request.

Required behavior:

- client declares desired capabilities
- server returns the exact supported subset it is willing to honor
- client gates optional behavior on the negotiated result

Capabilities should only be advertised when their semantics are actually implemented.

## 10. Hashes must be meaningful protocol values

Metadata and data hashes should not be empty placeholders.

Required behavior:

- extent descriptors carry real payload hashes
- file metadata carries useful content/version metadata when applicable
- stale/mismatch behavior is explicit, not implicit

Hash fields should exist to support validation and version binding, not just schema shape.

## Release-Shape RPC Roles

### `Attach`

Purpose:

- negotiate major protocol version
- negotiate exact capabilities
- return server identity and session-level metadata

### `Resolve`

Purpose:

- map logical path to stable file identity and versioned inode/layout snapshot

### `Fetch`

Purpose:

- return extent payloads for a specific inode generation

### `Stat`

Purpose:

- convenience metadata view for adapters

Constraint:

- must be derived from the same catalog state as `Resolve`

### `ListDir`

Purpose:

- convenience directory enumeration for adapters

Constraint:

- must be derived from the same catalog/replay state as other metadata operations

### `SubscribeChanges`

Purpose:

- authoritative ordered catalog replay from durable cursor

Release semantics:

- live stream
- ordered by sequence
- durable resume after reconnect
- explicit batch commit boundaries

### `Subscribe`

Purpose:

- best-effort invalidation nudge channel

Release semantics:

- optional
- coarse invalidation allowed
- correctness does not depend on it

### `Hint`

Release semantics:

- not advertised until real end-to-end residency semantics exist

## Client Correctness Rules

The client must obey these rules:

- do not treat path as identity
- do not fetch against an unresolved or stale inode generation
- do not treat invalidation as sufficient durable sync
- persist replay cursor durably
- rebuild metadata state from replay/checkpoint, not from ad hoc path probes alone
- keep authoritative extent-map knowledge separate from local residency state

## Rename, Update, And Delete Semantics

These need to be explicit because they drive cache correctness.

### Rename

- keep `file_id`
- update path mappings
- update directory membership
- invalidate old path view
- preserve local resident extents if inode generation remains valid

### Content update in place

- keep `file_id`
- advance inode generation
- publish new extent map
- stale old generation for future fetches
- drop or quarantine residency that no longer matches the active extent map

### Delete

- remove active path mapping
- tombstone file identity
- future fetches fail

### Delete and recreate at same path

- old `file_id` remains deleted
- new object gets new `file_id`

## Error Semantics

The protocol should make stale-state behavior obvious.

Required mapping:

- `NOT_FOUND`
  unknown path, directory entry, or deleted file identity
- `FAILED_PRECONDITION`
  stale inode generation or invalid fetch binding
- `UNAVAILABLE`
  transient transport/server outage
- `PERMISSION_DENIED`
  authn/authz failure
- `INVALID_ARGUMENT`
  malformed request that could never succeed

The client should only retry transient classes automatically.

## Deliberate Boundaries

The hardened protocol does not try to do these things:

- writable mutation through the mounted filesystem
- multi-server ownership
- shared cross-client residency management
- server-side fake acknowledgment of client-local residency work
- path-as-identity shortcuts

## Current Implementation Gaps To Close

These are the most important mismatches between the target protocol above and the current code:

1. `file_id` is derived from path today, so rename changes identity.
2. `Fetch` is not bound to an inode generation and can observe current server state rather than resolved state.
3. `SubscribeChanges` is a finite replay helper, not the live authoritative coherence stream.
4. The normal client runtime uses invalidation plus ad hoc re-resolve rather than durable ordered replay.
5. The client store handles subtree invalidation too bluntly and does not fully model recursive persisted invalidation behavior.
6. `Hint` is advertised but not implemented as a real residency protocol.
7. Wire-visible paths still reflect server-local deployment paths.
8. Hash-bearing metadata fields are still weak or placeholder values in several places.

## Immediate Protocol Work

The next protocol-hardening implementation work should be:

1. Replace path-derived `file_id` with persistent server-owned identity.
2. Add inode generation to `Resolve` and bind `Fetch` to it.
3. Convert `SubscribeChanges` into the authoritative live replay stream with durable cursor use.
4. Reclassify `Subscribe` as best-effort invalidation only.
5. Remove or disable `Hint` capability until it is real.
6. Move wire paths to a logical library namespace.
7. Separate authoritative client metadata state from local residency state.

## Bottom Line

Legato can be a strong protocol for DAW-oriented sample access, but only if identity, replay, and fetch versioning are made explicit and durable.

Without that hardening, the system behaves more like a convenient read-through transport than a release-quality replicated filesystem protocol.
