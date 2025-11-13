//! WebTransport server implementation for Bifrost.

mod cert;
mod session;

pub use cert::load_certificates;
pub use session::handle_session;
use tokio::time::sleep;

use crate::tpu_client::{LeaderTracker, TpuConnectionManager};
use anyhow::{Context, Result};
use log::info;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// WebTransport server that accepts connections and forwards transactions to TPU.
pub struct BifrostServer {
    addr: SocketAddr,
    cert_path: String,
    key_path: String,
}

impl BifrostServer {
    /// Creates a new Bifrost server instance.
    ///
    /// # Arguments
    ///
    /// * `addr` - Socket address to bind the server
    /// * `cert_path` - Path to TLS certificate file
    /// * `key_path` - Path to TLS private key file
    pub fn new(addr: SocketAddr, cert_path: &str, key_path: &str) -> Self {
        Self {
            addr,
            cert_path: cert_path.to_string(),
            key_path: key_path.to_string(),
        }
    }

    /// Starts the WebTransport server and begins accepting connections.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Certificate loading fails
    /// - TPU manager initialization fails
    /// - Server binding fails
    pub async fn run(self) -> Result<()> {
        info!("Starting Bifrost on {}", self.addr);

        let (cert_chain, private_key) = load_certificates(&self.cert_path, &self.key_path)
            .context("Failed to load certificates")?;

        // Initialize the LeaderTracker
        let leader_tracker = Arc::new(LeaderTracker::new().await);

        // Spawn the slot_updates listener as a background task
        let leader_tracker_clone = leader_tracker.clone();
        tokio::spawn(async move { LeaderTracker::run(leader_tracker_clone).await });

        // Spawn task to update leader sockets list every minute
        let leader_tracker_clone = leader_tracker.clone();
        tokio::spawn(async move {
            LeaderTracker::update_leader_sockets(leader_tracker_clone).await;

            sleep(Duration::from_secs(60)).await;
        });

        let tpu_manager = Arc::new(
            TpuConnectionManager::new(leader_tracker.clone())
                .context("Failed to create TPU manager")?,
        );

        let mut server = web_transport_quinn::ServerBuilder::new()
            .with_addr(self.addr)
            .with_certificate(cert_chain, private_key)?;

        info!("Listening for WebTransport connections");
        info!("Waiting for first connection");

        while let Some(request) = server.accept().await {
            info!("Server received request: {}", request.url());

            let tpu = tpu_manager.clone();
            tokio::spawn(async move {
                match request.ok().await {
                    Ok(session) => {
                        info!("Session accepted");
                        if let Err(e) = handle_session(session, tpu).await {
                            log::error!("Session error: {}", e);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to accept session: {}", e);
                    }
                }
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_creation() {
        let addr = "127.0.0.1:4433".parse().unwrap();
        let server = BifrostServer::new(addr, "certs/cert.pem", "certs/key.pem");
        assert_eq!(server.addr.port(), 4433);
    }

    #[tokio::test]
    async fn test_tpu_client_creation() {
        use crate::tpu_client::TpuConnectionManager;
        let leader_tracker = Arc::new(LeaderTracker::new().await);
        let result = TpuConnectionManager::new(leader_tracker);
        match result {
            Ok(_) => println!("TPU client created successfully"),
            Err(e) => panic!("TPU client failed: {}", e),
        }
    }
}
