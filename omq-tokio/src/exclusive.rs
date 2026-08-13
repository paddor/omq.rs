//! Exclusively owned, direct-I/O sockets for latency-sensitive callers.
//!
//! Unlike [`crate::Socket`], these sockets are deliberately not cloneable and
//! require `&mut self` for data-plane operations. This lets the caller poll
//! TCP and the ZMTP codec directly, without a connection-driver task or relay
//! ring. The initial prototype supports one connected NULL-mechanism DEALER.

use bytes::{Bytes, BytesMut};
use omq_proto::proto::{Connection, ConnectionConfig, Event, Role};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::{Error, Message, Result, SocketType};

/// A single-peer DEALER whose caller owns the TCP data path.
#[derive(Debug)]
pub struct ExclusiveDealer {
    stream: TcpStream,
    connection: Connection,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl ExclusiveDealer {
    /// Connect and complete the ZMTP NULL handshake.
    pub async fn connect(address: impl tokio::net::ToSocketAddrs, identity: Bytes) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        let connection = Connection::new(
            ConnectionConfig::new(Role::Client, SocketType::Dealer).identity(identity),
        );
        let mut dealer = Self {
            stream,
            connection,
            read_buf: BytesMut::with_capacity(4 * 1024),
            write_buf: BytesMut::with_capacity(4 * 1024),
        };
        dealer.finish_handshake().await?;
        Ok(dealer)
    }

    async fn finish_handshake(&mut self) -> Result<()> {
        while !self.connection.is_ready() {
            self.flush_connection().await?;
            if self.connection.is_ready() {
                break;
            }
            self.read_once().await?;
            while let Some(event) = self.connection.poll_event() {
                if let Event::HandshakeSucceeded { .. } = event {
                    break;
                }
            }
        }
        self.flush_connection().await
    }

    /// Encode and write one complete ZMTP message directly to TCP.
    pub async fn send(&mut self, message: &Message) -> Result<()> {
        self.write_buf.clear();
        self.connection
            .send_message_flat(message, &mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        Ok(())
    }

    /// Read and decode the next complete message directly from TCP.
    pub async fn recv(&mut self) -> Result<Message> {
        loop {
            if let Some(message) = self.connection.poll_message() {
                return Ok(message);
            }
            self.read_once().await?;
            // PING processing can queue PONG while decoding input.
            self.flush_connection().await?;
        }
    }

    async fn read_once(&mut self) -> Result<()> {
        let n = self.stream.read_buf(&mut self.read_buf).await?;
        if n == 0 {
            return Err(Error::Closed);
        }
        self.connection.handle_input(self.read_buf.split().freeze())
    }

    async fn flush_connection(&mut self) -> Result<()> {
        while self.connection.has_pending_transmit() {
            let chunks = self.connection.transmit_chunks_capped(64);
            let n = self.stream.write_vectored(&chunks).await?;
            drop(chunks);
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "exclusive DEALER write returned zero",
                )));
            }
            self.connection.advance_transmit(n);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Endpoint, Options, Socket};

    #[tokio::test]
    async fn dealer_round_trips_with_standard_router() {
        let router = Socket::new(SocketType::Router, Options::default());
        let bound = router
            .bind("tcp://127.0.0.1:0".parse::<Endpoint>().unwrap())
            .await
            .unwrap();
        let Endpoint::Tcp { host, port } = bound else {
            panic!("expected TCP endpoint")
        };
        let server = tokio::spawn(async move {
            for _ in 0..100 {
                let message = router.recv().await.unwrap();
                router.send(message).await.unwrap();
            }
        });
        let mut dealer = ExclusiveDealer::connect(
            format!("{host}:{port}"),
            Bytes::from_static(b"exclusive-test"),
        )
        .await
        .unwrap();
        for sequence in 0_u64..100 {
            let message = Message::single(Bytes::copy_from_slice(&sequence.to_le_bytes()));
            dealer.send(&message).await.unwrap();
            assert_eq!(dealer.recv().await.unwrap(), message);
        }
        server.await.unwrap();
    }
}
