//! # Bifrost
//!
//! A WebTransport proxy for Solana TPU (Transaction Processing Unit) connections.
//!
//! Bifrost enables browsers and clients to send Solana transactions directly to
//! validator TPUs using WebTransport protocol, bypassing traditional RPC endpoints.
//!
//! ## Features
//!
//! - WebTransport server for browser connectivity
//! - Direct TPU connection management
//! - Transaction forwarding from WebTransport to QUIC/UDP
//!
//! ## Example
//!
//! ```no_run
//! use bifrost::server::BifrostServer;
//! use std::net::SocketAddr;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let addr: SocketAddr = "[::]:4433".parse()?;
//!     let server = BifrostServer::new(addr, "certs/cert.pem", "certs/key.pem");
//!     server.run().await?;
//!     Ok(())
//! }
//! ```
//!


pub mod server;
pub mod tpu_client;
pub mod constants;

pub use server::BifrostServer;
pub use tpu_client::TpuConnectionManager;

pub type Slot = u64;
