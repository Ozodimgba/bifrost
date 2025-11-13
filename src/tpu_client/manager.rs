use anyhow::{Context, Result};
use dashmap::DashMap;
use log::{debug, info};
use quinn::{
    ClientConfig, Connection, Endpoint, IdleTimeout, TransportConfig,
    crypto::rustls::QuicClientConfig,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::u8;

use crate::tpu_client::LeaderTracker;

const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";
const QUIC_MAX_TIMEOUT: Duration = Duration::from_secs(30);
const QUIC_KEEP_ALIVE: Duration = Duration::from_secs(5);

/// Result of a transaction delivery attempt.
#[derive(Debug, Clone)]
pub struct DeliveryConfirmation {
    pub delivered: bool,
    pub latency: Duration,
}

/// Manages QUIC connections to Solana TPU endpoints.
///
/// Maintains a connection pool and handles automatic reconnection.
#[derive(Debug)]
pub struct TpuConnectionManager {
    endpoint: Endpoint,
    connections: Arc<DashMap<String, Connection>>,
    leader_tracker: Arc<LeaderTracker>,
}

impl TpuConnectionManager {
    /// Creates a new TPU connection manager.
    ///
    /// # Errors
    ///
    /// Returns an error if the QUIC endpoint cannot be initialized.
    pub fn new(leader_tracker: Arc<LeaderTracker>) -> Result<Self> {
        info!("Creating TPU connection manager");

        let client_certificate = solana_tls_utils::QuicClientCertificate::new(None);

        let mut crypto = solana_tls_utils::tls_client_config_builder()
            .with_client_auth_cert(
                vec![client_certificate.certificate.clone()],
                client_certificate.key.clone_key(),
            )
            .expect("Failed to set QUIC client certificates");

        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let transport_config = {
            let mut res = TransportConfig::default();
            let timeout = IdleTimeout::try_from(QUIC_MAX_TIMEOUT).unwrap();
            res.max_idle_timeout(Some(timeout));
            res.keep_alive_interval(Some(QUIC_KEEP_ALIVE));
            res.send_fairness(false);
            res
        };

        let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
        config.transport_config(Arc::new(transport_config));

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
        endpoint.set_default_client_config(config);

        info!("TPU connection manager created");

        Ok(Self {
            endpoint,
            connections: Arc::new(DashMap::new()),
            leader_tracker,
        })
    }

    /// Sends a Solana transaction to the specified validator's TPU.
    ///
    /// # Arguments
    ///
    /// * `validator` - TPU address (e.g., "127.0.0.1:8001")
    /// * `transaction` - The transaction to send
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Serialization fails
    /// - Connection fails
    /// - Stream creation fails
    /// - Data transmission fails
    pub async fn send_transaction(
        &self,
        validator: &str,
        tx_data: &[u8],
    ) -> Result<DeliveryConfirmation> {
        let start = Instant::now();

        info!("Sending {} bytes to {}", tx_data.len(), validator);
        debug!("Packet preview: {:02x?}", &tx_data[..tx_data.len().min(32)]);

        let connection = self.get_or_create_connection(validator).await?;

        let mut send_stream = connection
            .open_uni()
            .await
            .context("Failed to open uni stream")?;

        send_stream
            .write_all(&tx_data)
            .await
            .context("Failed to write transaction data")?;

        send_stream.finish().context("Failed to finish stream")?;

        let latency = start.elapsed();
        debug!("Transaction sent in {:?}", latency);

        Ok(DeliveryConfirmation {
            delivered: true,
            latency,
        })
    }

    /// Gets an existing connection or creates a new one to the validator.
    /// TODO: Connect to future leaders based on LeaderTracker
    async fn get_or_create_connection(&self, validator: &str) -> Result<Connection> {
        if let Some(conn) = self.connections.get(validator) {
            if !conn.close_reason().is_some() {
                debug!("Reusing connection to {}", validator);
                return Ok(conn.clone());
            }
        }

        info!("Creating new connection to {}", validator);
        let addr: SocketAddr = validator.parse().context("Invalid validator address")?;

        let connection = self
            .endpoint
            .connect(addr, "solana")?
            .await
            .context("Failed to connect to validator")?;

        self.connections.insert(validator.to_string(), connection.clone());
        info!("Connected to {}", validator);

        Ok(connection)
    }


    /// Returns the number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Closes all connections.
    pub fn close_all(&self) {
        for conn in self.connections.iter() {
            conn.value().close(0u32.into(), b"shutdown");
        }
        self.connections.clear();
    }
}

impl Drop for TpuConnectionManager {
    fn drop(&mut self) {
        self.close_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_creation() {
        let leader_tracker = Arc::new(LeaderTracker::default());
        let manager = TpuConnectionManager::new(leader_tracker);
        assert!(manager.is_ok());
    }

    #[test]
    fn test_connection_count() {
        let leader_tracker = Arc::new(LeaderTracker::default());
        let manager = TpuConnectionManager::new(leader_tracker).unwrap();
        assert_eq!(manager.connection_count(), 0);
    }
}
