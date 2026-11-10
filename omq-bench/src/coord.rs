use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use omq_tokio::blocking::Socket;
use omq_tokio::{Context, Message, Options, SocketType};

static COORD_CTX: OnceLock<Context> = OnceLock::new();
static COORD_COUNTER: AtomicU64 = AtomicU64::new(0);

fn coord_ctx() -> &'static Context {
    COORD_CTX.get_or_init(Context::new)
}

fn coord_ipc_endpoint() -> String {
    let pid = std::process::id();
    let n = COORD_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ipc:///tmp/omq-bench-coord-{pid}-{n}")
}

pub(crate) struct CoordSocket {
    sock: Socket,
    endpoint: String,
}

impl CoordSocket {
    pub(crate) fn bind_new() -> Self {
        let sock = coord_ctx().blocking_socket(SocketType::Pull, Options::default());
        let endpoint = coord_ipc_endpoint();
        let ep: omq_tokio::Endpoint = endpoint.parse().unwrap();
        sock.bind(ep).expect("coord bind");

        Self { sock, endpoint }
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn recv_ready_port(&self, timeout: Duration) -> Option<u16> {
        let sock = self.sock.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        std::thread::spawn(move || {
            if let Ok(msg) = sock.recv() {
                tx.send(msg).ok();
            }
        });

        let msg = rx.recv_timeout(timeout).ok()?;
        parse_ready_port(&msg)
    }
}

impl Drop for CoordSocket {
    fn drop(&mut self) {
        if let Some(path) = self.endpoint.strip_prefix("ipc://") {
            std::fs::remove_file(path).ok();
        }
    }
}

fn parse_ready_port(msg: &Message) -> Option<u16> {
    let bytes = msg.part_bytes(0)?;
    let text = std::str::from_utf8(&bytes).ok()?;
    text.strip_prefix("READY ")?.trim().parse().ok()
}
