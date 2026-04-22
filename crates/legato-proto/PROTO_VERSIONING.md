# Protocol Versioning

This is a focused wire-compatibility note. For the top-level project index, start at [README.md](../../README.md) or [docs/INDEX.md](../../docs/INDEX.md).

Legato uses protobuf package versioning for wire compatibility.

Current namespace:

- `legato.v1`

Rules:

1. Additive changes within `legato.v1` are allowed when they preserve protobuf compatibility.
2. Field numbers in `legato.v1` are never reused.
3. Removed fields must be reserved before deletion.
4. Semantics that require a breaking interpretation change must ship in a new namespace such as `legato.v2`.
5. `AttachRequest.protocol_version` and `AttachResponse.protocol_version` communicate the negotiated major protocol version at runtime.
6. Capability negotiation is explicit through `desired_capabilities` and `negotiated_capabilities`; unsupported optional behavior should fail at capability negotiation rather than implicit runtime assumptions.

Client and server compatibility contract:

- same major version: required
- lower client major version than server: only supported if the server still serves that namespace
- higher client major version than server: rejected during attach negotiation
