//! Quinn endpoint plus a handle on its UDP socket for kernel buffer sizes.

use std::sync::Mutex;

use omq_proto::{Error, Result};

/// One UDP socket and the Quinn endpoint driving it. Every QUIC connection
/// of the endpoint shares the socket and its kernel buffers.
pub(crate) struct UdpEndpoint {
    quinn: quinn::Endpoint,
    /// Duplicate handle on the endpoint's socket. Quinn does not expose the
    /// socket, and options set through either handle apply to both.
    socket: std::net::UdpSocket,
    /// Largest `SO_RCVBUF` and `SO_SNDBUF` requested so far.
    buffers: Mutex<(usize, usize)>,
}

impl UdpEndpoint {
    /// Wrap `socket` in a Quinn endpoint. Must run on the IO runtime that
    /// drives the endpoint.
    pub(super) fn new(
        socket: std::net::UdpSocket,
        config: quinn::EndpointConfig,
        server: Option<quinn::ServerConfig>,
    ) -> Result<Self> {
        let handle = socket.try_clone().map_err(Error::Io)?;
        let runtime = quinn::default_runtime()
            .ok_or_else(|| Error::Io(std::io::Error::other("no async runtime for QUIC")))?;
        let quinn = quinn::Endpoint::new(config, server, socket, runtime).map_err(Error::Io)?;
        Ok(Self {
            quinn,
            socket: handle,
            buffers: Mutex::new((0, 0)),
        })
    }

    /// Raise the socket's kernel buffers to the requested sizes. A size at
    /// or below an earlier request is ignored, so peers sharing the socket
    /// get the largest size any of them asked for. Best effort, as for TCP:
    /// Linux caps sizes at `net.core.rmem_max` / `net.core.wmem_max`, and
    /// BSD-derived systems reject sizes above `kern.ipc.maxsockbuf`, keeping
    /// the previous size.
    pub(super) fn raise_buffers(&self, recv: Option<usize>, send: Option<usize>) {
        let mut buffers = self.buffers.lock().expect("UDP buffer sizes");
        let socket = socket2::SockRef::from(&self.socket);
        if let Some(n) = recv
            && n > buffers.0
        {
            buffers.0 = n;
            let _ = socket.set_recv_buffer_size(n);
        }
        if let Some(n) = send
            && n > buffers.1
        {
            buffers.1 = n;
            let _ = socket.set_send_buffer_size(n);
        }
    }

    /// Kernel `SO_RCVBUF` and `SO_SNDBUF` as the socket reports them.
    #[cfg(test)]
    pub(super) fn buffer_sizes(&self) -> (usize, usize) {
        let socket = socket2::SockRef::from(&self.socket);
        (
            socket.recv_buffer_size().unwrap(),
            socket.send_buffer_size().unwrap(),
        )
    }
}

impl std::ops::Deref for UdpEndpoint {
    type Target = quinn::Endpoint;

    fn deref(&self) -> &quinn::Endpoint {
        &self.quinn
    }
}

impl std::fmt::Debug for UdpEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpEndpoint")
            .field("local_addr", &self.quinn.local_addr().ok())
            .finish_non_exhaustive()
    }
}
