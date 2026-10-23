//! TCP socket settings applied after connect or accept.

use super::Options;
use crate::socket_ref::SocketRef;
use std::time::Duration;

/// TCP keepalive policy. `Default` leaves the OS defaults alone (matches
/// libzmq's `ZMQ_TCP_KEEPALIVE = -1`); `Disabled` clears `SO_KEEPALIVE`;
/// `Enabled` sets `SO_KEEPALIVE` and pins the three timing knobs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeepAlive {
    /// OS defaults; nothing applied to the socket.
    #[default]
    Default,
    /// Explicitly disable `SO_KEEPALIVE`.
    Disabled,
    /// Enable `SO_KEEPALIVE` and set the timing triplet.
    Enabled {
        /// Idle time before the first probe is sent (`TCP_KEEPIDLE`).
        idle: Duration,
        /// Interval between probes (`TCP_KEEPINTVL`).
        intvl: Duration,
        /// Failed probes before declaring the connection dead (`TCP_KEEPCNT`).
        cnt: u32,
    },
}

impl Options {
    /// Apply `SO_RCVBUF` and `SO_SNDBUF` to a connected socket.
    pub fn apply_socket_buffers<S: SocketRef>(&self, sock: &S) -> std::io::Result<()> {
        let sref = sock.as_socket_ref();
        if let Some(n) = self.recv_buffer_size {
            sref.set_recv_buffer_size(n)?;
        }
        if let Some(n) = self.send_buffer_size {
            sref.set_send_buffer_size(n)?;
        }
        Ok(())
    }

    /// Apply [`Self::tcp_user_timeout`] to a connected TCP socket. No-op
    /// where the OS has no `TCP_USER_TIMEOUT`.
    pub fn apply_tcp_user_timeout<S: SocketRef>(&self, sock: &S) -> std::io::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(timeout) = self.tcp_user_timeout {
            sock.as_socket_ref().set_tcp_user_timeout(Some(timeout))?;
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let _ = sock;
        Ok(())
    }
}

impl KeepAlive {
    /// Apply this keepalive policy to a connected TCP socket after
    /// `connect`/`accept` so the option is in effect for the
    /// connection's lifetime.
    pub fn apply<S: SocketRef>(&self, sock: &S) -> std::io::Result<()> {
        let sref = sock.as_socket_ref();
        match self {
            KeepAlive::Default => Ok(()),
            KeepAlive::Disabled => sref.set_keepalive(false),
            KeepAlive::Enabled { idle, intvl, cnt } => {
                let ka = socket2::TcpKeepalive::new()
                    .with_time(*idle)
                    .with_interval(*intvl)
                    .with_retries(*cnt);
                sref.set_tcp_keepalive(&ka)
            }
        }
    }
}
