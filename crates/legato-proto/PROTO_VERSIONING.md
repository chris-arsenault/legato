# Protocol Versioning

This is a focused protocol note. For the top-level project index, start at [README.md](../../README.md) or [docs/INDEX.md](../../docs/INDEX.md).

Legato uses protobuf package versioning.

Current namespace:

- `legato.v1`

Rules:

1. Additive changes within `legato.v1` are allowed when they preserve existing field meanings.
2. Field numbers in `legato.v1` are never reused.
3. Removed fields must be reserved before deletion.
4. Semantics that require a breaking interpretation change ship in a new namespace such as `legato.v2`.
5. `AttachRequest.protocol_version` and `AttachResponse.protocol_version` communicate the negotiated major protocol version at runtime.
6. Capability negotiation is explicit through `desired_capabilities` and `negotiated_capabilities`.

Protocol vocabulary:

- `Attach`
  Session setup and capability negotiation.
- `Resolve`
  Path to inode and extent-map resolution.
- `Fetch`
  Extent record streaming.
- `Hint`
  Prioritized residency requests.
- `Subscribe`
  Ordered record stream replay after a sequence cursor.

Transfer contract:

- file identity is logical and server-assigned
- inode records carry metadata and layout
- extent maps describe semantic byte ranges
- extent records carry payload hashes
- clients track local residency for partial replicas
