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
        Capability::BlockStreaming as i32,
        Capability::Prefetch as i32,
        Capability::Invalidations as i32,
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        AttachRequest, AttachResponse, Capability, PROTOCOL_NAMESPACE, PROTOCOL_VERSION,
        default_capabilities,
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
            negotiated_capabilities: request.desired_capabilities.clone(),
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
    }
}
