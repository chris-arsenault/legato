//! Shared protocol-facing types and compatibility helpers.

use legato_types::PrefetchRequest;

/// Initial wire compatibility version for the Legato RPC surface.
pub const PROTOCOL_VERSION: u32 = 1;

/// Minimal client/server attach information used during early bootstrap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachRequest {
    /// Version of the protocol the caller expects to speak.
    pub protocol_version: u32,
    /// Human-readable component name for diagnostics.
    pub client_name: String,
}

/// Server response describing negotiated compatibility.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachResponse {
    /// Protocol version the server accepted for the connection.
    pub protocol_version: u32,
}

/// Local control command forwarded from the planner to the mounted client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutePrefetch {
    /// Planner-derived request to make local cache entries resident.
    pub request: PrefetchRequest,
}

#[cfg(test)]
mod tests {
    use super::{AttachRequest, AttachResponse, PROTOCOL_VERSION};

    #[test]
    fn attach_round_trip_uses_workspace_protocol_version() {
        let request = AttachRequest {
            protocol_version: PROTOCOL_VERSION,
            client_name: String::from("legatofs"),
        };
        let response = AttachResponse {
            protocol_version: request.protocol_version,
        };

        assert_eq!(response.protocol_version, PROTOCOL_VERSION);
        assert_eq!(request.client_name, "legatofs");
    }
}
