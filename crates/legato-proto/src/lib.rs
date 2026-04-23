//! Shared protocol-facing types and compatibility helpers.

/// Initial wire compatibility version for the Legato RPC surface.
pub const PROTOCOL_VERSION: u32 = 1;

/// Canonical protobuf namespace for the current major protocol version.
pub const PROTOCOL_NAMESPACE: &str = "legato.v1";

/// Generated protobuf types and gRPC stubs for the `legato.v1` namespace.
pub mod legato {
    /// Generated items for version 1 of the Legato protocol.
    #[allow(missing_docs)]
    pub mod v1 {
        tonic::include_proto!("legato.v1");
    }
}

pub use legato::v1::*;

/// Returns the default client capability set expected during bootstrap.
#[must_use]
pub fn default_capabilities() -> Vec<i32> {
    vec![
        Capability::Metadata as i32,
        Capability::Resolve as i32,
        Capability::ExtentFetch as i32,
        Capability::Prefetch as i32,
        Capability::Invalidations as i32,
        Capability::ChangeSubscription as i32,
    ]
}

/// Returns the exact supported subset of the requested capabilities.
#[must_use]
pub fn negotiate_capabilities(requested: &[i32]) -> Vec<i32> {
    let supported = default_capabilities();
    requested
        .iter()
        .copied()
        .filter(|capability| supported.contains(capability))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        AttachRequest, AttachResponse, Capability, ChangeKind, ExtentDescriptor, ExtentRecord,
        FileLayout, HintExtent, HintRequest, InodeMetadata, PROTOCOL_NAMESPACE, PROTOCOL_VERSION,
        PrefetchPriority, ResolveRequest, ResolveResponse, TransferClass, default_capabilities,
        negotiate_capabilities,
    };

    #[test]
    fn attach_round_trip_uses_workspace_protocol_version() {
        let request = AttachRequest {
            protocol_version: PROTOCOL_VERSION,
            client_name: String::from("legatofs"),
            desired_capabilities: default_capabilities(),
        };
        let response = AttachResponse {
            protocol_version: request.protocol_version,
            negotiated_capabilities: negotiate_capabilities(&request.desired_capabilities),
            server_name: String::from("legato-server"),
        };

        assert_eq!(response.protocol_version, PROTOCOL_VERSION);
        assert_eq!(request.client_name, "legatofs");
        assert_eq!(PROTOCOL_NAMESPACE, "legato.v1");
        assert!(
            response
                .negotiated_capabilities
                .contains(&(Capability::Prefetch as i32))
        );
        assert!(
            !response
                .negotiated_capabilities
                .contains(&(Capability::Hint as i32))
        );
        assert!(
            response
                .negotiated_capabilities
                .contains(&(Capability::Resolve as i32))
        );
        assert!(
            response
                .negotiated_capabilities
                .contains(&(Capability::ExtentFetch as i32))
        );
    }

    #[test]
    fn resolve_response_can_carry_semantic_layout_metadata() {
        let request = ResolveRequest {
            path: String::from("/srv/libraries/Kontakt/piano.nki"),
        };
        let response = ResolveResponse {
            inode: Some(InodeMetadata {
                file_id: 7,
                path: request.path.clone(),
                size: 4_096,
                mtime_ns: 99,
                is_dir: false,
                layout: Some(FileLayout {
                    transfer_class: TransferClass::Streamed as i32,
                    extents: vec![
                        ExtentDescriptor {
                            extent_index: 0,
                            file_offset: 0,
                            length: 2_048,
                            extent_hash: b"extent-0".to_vec(),
                        },
                        ExtentDescriptor {
                            extent_index: 1,
                            file_offset: 2_048,
                            length: 2_048,
                            extent_hash: b"extent-1".to_vec(),
                        },
                    ],
                }),
                inode_generation: 3,
                content_hash: b"content-hash".to_vec(),
            }),
        };

        let inode = response.inode.expect("inode should be present");
        let layout = inode.layout.expect("layout should be present");
        assert_eq!(inode.path, request.path);
        assert_eq!(inode.inode_generation, 3);
        assert_eq!(inode.content_hash, b"content-hash".to_vec());
        assert_eq!(layout.transfer_class, TransferClass::Streamed as i32);
        assert_eq!(layout.extents.len(), 2);
    }

    #[test]
    fn hint_requests_and_extent_records_use_reset_protocol_vocabulary() {
        let request = HintRequest {
            extents: vec![HintExtent {
                file_id: 42,
                extent_index: 3,
                file_offset: 12_288,
                length: 4_096,
                priority: PrefetchPriority::P0 as i32,
                deadline_unix_ms: 123,
            }],
            wait_for_residency: true,
            wait_through_priority: PrefetchPriority::P1 as i32,
        };
        let record = ExtentRecord {
            file_id: 42,
            extent_index: 3,
            file_offset: 12_288,
            data: b"legato".to_vec(),
            extent_hash: b"hash".to_vec(),
            transfer_class: TransferClass::Streamed as i32,
        };

        assert_eq!(request.extents.len(), 1);
        assert!(request.wait_for_residency);
        assert_eq!(record.transfer_class, TransferClass::Streamed as i32);
    }

    #[test]
    fn change_kind_captures_ordered_catalog_semantics() {
        assert_eq!(ChangeKind::Upsert as i32, 1);
        assert_eq!(ChangeKind::Delete as i32, 2);
        assert_eq!(ChangeKind::Invalidate as i32, 3);
        assert_eq!(ChangeKind::Checkpoint as i32, 4);
    }
}
