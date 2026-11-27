//! Benchmark transports: endpoints, case name prefixes, and options.

#[cfg(feature = "quic")]
use std::process::Command;
#[cfg(feature = "quic")]
use std::sync::LazyLock;
use std::sync::atomic::Ordering;

use omq_tokio::{Endpoint, Options};

#[cfg(feature = "curve")]
const CURVE_SERVER_KEY: u8 = 0x01;
#[cfg(feature = "curve")]
const CURVE_CLIENT_KEY: u8 = 0x02;

#[cfg(feature = "quic")]
static QUIC_CREDENTIALS: LazyLock<(Vec<u8>, Vec<u8>)> = LazyLock::new(|| {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("omq-perf-tls-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&dir).expect("create benchmark TLS directory");
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");

    let output = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-out",
        ])
        .arg(&cert)
        .arg("-keyout")
        .arg(&key)
        .output()
        .expect("openssl is required for QUIC performance gates");

    assert!(
        output.status.success(),
        "generate benchmark TLS certificate: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let credentials = (
        std::fs::read(&cert).expect("read benchmark certificate"),
        std::fs::read(&key).expect("read benchmark key"),
    );
    std::fs::remove_dir_all(dir).expect("remove benchmark TLS directory");

    credentials
});

#[cfg(feature = "curve")]
fn curve_keypair(byte: u8) -> omq_tokio::CurveKeypair {
    let secret = omq_tokio::CurveSecretKey::from_bytes([byte; 32]);
    let public = secret.derive_public();
    omq_tokio::CurveKeypair { public, secret }
}

pub(super) fn tcp_zero() -> Endpoint {
    "tcp://127.0.0.1:0".parse().expect("valid TCP endpoint")
}

fn inproc_endpoint() -> Endpoint {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    format!(
        "inproc://perf-gate-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
    .parse()
    .expect("valid inproc endpoint")
}

#[cfg(unix)]
fn local_endpoint(scheme: &str) -> Endpoint {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("omq-perf-{}-{id}.{scheme}", std::process::id()));

    format!("{scheme}://{}", path.display())
        .parse()
        .expect("local endpoint")
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Transport {
    Tcp,
    Inproc,
    #[cfg(unix)]
    Ipc,
    #[cfg(target_os = "linux")]
    Shm,
    #[cfg(feature = "quic")]
    Quic,
    #[cfg(feature = "dart")]
    Dart,
    /// TCP with CURVE. The binding socket is the server.
    #[cfg(feature = "curve")]
    Curve,
}

impl Transport {
    pub(super) fn prefix(self) -> &'static str {
        match self {
            Self::Tcp => "",
            Self::Inproc => "inproc_",
            #[cfg(unix)]
            Self::Ipc => "ipc_",
            #[cfg(target_os = "linux")]
            Self::Shm => "shm_",
            #[cfg(feature = "quic")]
            Self::Quic => "quic_",
            #[cfg(feature = "dart")]
            Self::Dart => "dart_",
            #[cfg(feature = "curve")]
            Self::Curve => "curve_",
        }
    }

    pub(super) fn endpoint(self) -> Endpoint {
        match self {
            Self::Tcp => tcp_zero(),
            Self::Inproc => inproc_endpoint(),
            #[cfg(unix)]
            Self::Ipc => local_endpoint("ipc"),
            #[cfg(target_os = "linux")]
            Self::Shm => local_endpoint("shm"),
            #[cfg(feature = "quic")]
            Self::Quic => "quic://127.0.0.1:0".parse().expect("QUIC endpoint"),
            #[cfg(feature = "dart")]
            Self::Dart => "dart://127.0.0.1:0".parse().expect("DART endpoint"),
            #[cfg(feature = "curve")]
            Self::Curve => tcp_zero(),
        }
    }

    pub(super) fn options(self) -> Options {
        match self {
            #[cfg(feature = "quic")]
            Self::Quic => {
                let (cert, key) = &*QUIC_CREDENTIALS;
                Options {
                    quic: omq_tokio::options::QuicOptions {
                        server_cert_pem: Some(cert.clone()),
                        server_key_pem: Some(key.clone()),
                        trust_pem: Some(cert.clone()),
                        trust_system: false,
                        ..Default::default()
                    },
                    recv_buffer_size: Some(4 * 1024 * 1024),
                    send_buffer_size: Some(4 * 1024 * 1024),
                    ..Options::default()
                }
            }
            _ => Options::default(),
        }
    }

    /// Options for the binding socket: a CURVE binder is the server.
    pub(super) fn bind_options(self) -> Options {
        #[cfg(feature = "curve")]
        if let Self::Curve = self {
            return Options::default().curve_server(curve_keypair(CURVE_SERVER_KEY));
        }

        self.options()
    }

    /// Options for a connecting socket: a CURVE connector is a client of the
    /// binding server.
    pub(super) fn connect_options(self) -> Options {
        #[cfg(feature = "curve")]
        if let Self::Curve = self {
            return Options::default().curve_client(
                curve_keypair(CURVE_CLIENT_KEY),
                curve_keypair(CURVE_SERVER_KEY).public,
            );
        }

        self.options()
    }
}
