//! TURN 服务器实现
//!
//! 提供 TURN 中继服务器功能，用于 NAT 穿越和网络中继

// TURN server implementation modules
mod authenticator;
pub mod error;

// Re-export types for convenience
pub use actr_protocol::turn::{Claims, Token};
pub use authenticator::Authenticator;
pub use error::{ErrorSeverity, TurnError};

use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tracing::*;
use turn_crate::auth::AuthHandler;
use turn_crate::relay::relay_range::*;
use turn_crate::server::config::*;
use turn_crate::server::*;
use webrtc_util::vnet::net::*;

// Create and initialize the TURN server
pub async fn create_turn_server(
    socket: Arc<UdpSocket>,
    advertised_ip: &str,
    realm: &str,
    auth_handler: Arc<dyn AuthHandler + Send + Sync>,
) -> error::Result<Server> {
    info!("Creating TURN server with advertised IP: {}", advertised_ip);

    // Get the local address of the socket
    let local_addr = match socket.local_addr() {
        Ok(addr) => addr.ip().to_string(),
        Err(e) => {
            error!("Failed to get local address from socket: {}", e);
            // Fall back to 0.0.0.0 if we can't get the actual address
            "0.0.0.0".to_string()
        }
    };

    info!(
        "TURN server will use local address: {} and advertised IP: {}",
        local_addr, advertised_ip
    );

    // Parse advertised IP
    let relay_ip = match IpAddr::from_str(advertised_ip) {
        Ok(ip) => ip,
        Err(e) => {
            let err_msg = format!("Invalid advertised IP address: {e}");
            error!("{}", err_msg);
            return Err(TurnError::Configuration {
                field: "advertised_ip".to_string(),
                value: advertised_ip.to_string(),
            });
        }
    };

    // Create TURN server configuration with dynamic relay port range
    // Default ephemeral range: 49152-65535 (IANA recommended)
    let server_config = ServerConfig {
        conn_configs: vec![ConnConfig {
            conn: socket,
            relay_addr_generator: Box::new(RelayAddressGeneratorRanges {
                relay_address: relay_ip,
                min_port: 49152,
                max_port: 65535,
                max_retries: 10,
                address: local_addr,
                net: Arc::new(Net::new(None)),
            }),
        }],
        realm: realm.to_string(),
        auth_handler,
        channel_bind_timeout: std::time::Duration::from_secs(600), // 10 minutes
        alloc_close_notify: None, // No allocation close notification handler
    };

    // Create the actual server instance
    let server = match Server::new(server_config).await {
        Ok(server) => server,
        Err(e) => {
            let err_msg = format!("Failed to create TURN server: {e}");
            error!("{}", err_msg);
            return Err(TurnError::ServerStartFailed { reason: err_msg });
        }
    };

    info!("TURN server created successfully (includes STUN functionality)");
    Ok(server)
}

// Shutdown the TURN server
pub async fn shutdown_turn_server(server: &Server) -> error::Result<()> {
    info!("Shutting down TURN server");

    if let Err(e) = server.close().await {
        error!("Error while closing TURN server: {e}");
        return Err(TurnError::ServerShutdownFailed {
            reason: format!("Failed to close TURN server: {e}"),
        });
    }

    info!("TURN server has been shut down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::UdpSocket;
    use turn_crate::auth::AuthHandler;

    // Mock auth handler for testing
    struct MockAuthHandler;

    impl AuthHandler for MockAuthHandler {
        fn auth_handle(
            &self,
            _username: &str,
            _realm: &str,
            _src_addr: SocketAddr,
        ) -> Result<Vec<u8>, turn_crate::Error> {
            Ok(vec![0u8; 16]) // Return dummy hash
        }
    }

    #[tokio::test]
    async fn test_create_turn_server() -> anyhow::Result<()> {
        // Create a UDP socket for testing
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let auth_handler: Arc<dyn AuthHandler + Send + Sync> = Arc::new(MockAuthHandler);

        // Test server creation
        let server = create_turn_server(socket, "127.0.0.1", "test.realm", auth_handler).await?;

        // Test server shutdown
        shutdown_turn_server(&server).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_public_ip() {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let auth_handler: Arc<dyn AuthHandler + Send + Sync> = Arc::new(MockAuthHandler);

        // Test with invalid IP
        let result =
            create_turn_server(socket, "invalid.ip.address", "test.realm", auth_handler).await;

        assert!(result.is_err());
    }
}
