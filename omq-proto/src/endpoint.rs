//! Endpoint parsing and formatting.
//!
//! An [`Endpoint`] is a transport address like `tcp://host:port` or
//! `inproc://name`. An [`EndpointSpec`] is an endpoint with an optional
//! role prefix (`@` for bind, `>` for connect), useful for CLI-style
//! single-string specifications.

use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

#[cfg(unix)]
use std::path::PathBuf;

use crate::error::{Error, Result};

/// A transport endpoint.
///
/// The scheme picks the transport; the rest of the string carries transport-
/// specific addressing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Endpoint {
    /// `tcp://host:port` (IPv4, IPv6, or DNS name).
    Tcp {
        /// Host to resolve or bind.
        host: Host,
        /// TCP port.
        port: u16,
    },
    /// `ipc://path` filesystem socket (Unix), `ipc://@name` Linux abstract namespace,
    /// or `ipc://name` Windows named pipe.
    Ipc(IpcPath),
    /// `inproc://name` in-process transport.
    Inproc {
        /// In-process endpoint name.
        name: String,
    },
    /// `udp://[group@]host:port` for RADIO/DISH (group optional).
    Udp {
        /// Optional multicast group.
        group: Option<String>,
        /// Host or multicast address.
        host: Host,
        /// UDP port.
        port: u16,
    },
    /// `lz4+tcp://host:port` LZ4-compressed TCP. Requires the `lz4` feature.
    #[cfg(feature = "lz4")]
    Lz4Tcp { host: Host, port: u16 },
    /// `zstd+tcp://host:port` Zstd-compressed TCP. Requires the `zstd` feature.
    #[cfg(feature = "zstd")]
    ZstdTcp { host: Host, port: u16 },
    /// `ws://host:port/path` ZeroMQ over WebSocket (RFC 45). Requires the
    /// `ws` feature.
    #[cfg(feature = "ws")]
    Ws { host: Host, port: u16, path: String },
    /// `wss://host:port/path` ZeroMQ over WebSocket with TLS. Requires the
    /// `ws` feature.
    #[cfg(feature = "ws")]
    Wss { host: Host, port: u16, path: String },
    /// `lz4+ws://host:port/path` LZ4-compressed WebSocket. Requires the
    /// `lz4` and `ws` features.
    #[cfg(all(feature = "lz4", feature = "ws"))]
    Lz4Ws { host: Host, port: u16, path: String },
    /// `lz4+wss://host:port/path` LZ4-compressed WebSocket with TLS.
    /// Requires the `lz4` and `ws` features.
    #[cfg(all(feature = "lz4", feature = "ws"))]
    Lz4Wss { host: Host, port: u16, path: String },
}

/// TCP / UDP host specification: either an IP address or a DNS name.
///
/// Kept as a distinct variant so resolution can be deferred until bind/connect
/// time without forcing callers to reparse.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Host {
    /// Literal IP address.
    Ip(IpAddr),
    /// DNS name resolved at bind or connect time.
    Name(String),
    /// Wildcard (`0.0.0.0` / `::` / `*`) -- bind-only.
    Wildcard,
}

/// IPC path: filesystem socket (Unix), abstract namespace (Linux), or named pipe (Windows).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum IpcPath {
    /// Unix filesystem socket path.
    #[cfg(unix)]
    Filesystem(PathBuf),
    /// Linux abstract namespace (no filesystem entry).
    #[cfg(unix)]
    Abstract(String),
    /// Windows named pipe name (will become `\\.\ pipe\name` at bind/connect time).
    #[cfg(target_os = "windows")]
    NamedPipe(String),
}

/// Role for an endpoint in a single-string spec: bind, connect, or default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EndpointRole {
    /// `@endpoint` -- explicit bind.
    Bind,
    /// `>endpoint` -- explicit connect.
    Connect,
    /// No prefix -- socket type picks (PUSH connects, PULL binds, etc.).
    Default,
}

/// An endpoint plus an optional role prefix. Parsed from strings like
/// `"@tcp://*:5555"` or `">tcp://host:5555"` or just `"tcp://host:5555"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointSpec {
    /// Bind, connect, or socket-type default.
    pub role: EndpointRole,
    /// The transport address.
    pub endpoint: Endpoint,
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (scheme, rest) = s
            .split_once("://")
            .ok_or_else(|| Error::InvalidEndpoint(s.to_string()))?;

        match scheme {
            "tcp" => parse_host_port(rest).map(|(host, port)| Endpoint::Tcp { host, port }),
            "ipc" => Ok(Endpoint::Ipc(parse_ipc(rest)?)),
            "inproc" => {
                if rest.is_empty() {
                    return Err(Error::InvalidEndpoint(s.to_string()));
                }
                Ok(Endpoint::Inproc {
                    name: rest.to_string(),
                })
            }
            "udp" => parse_udp(rest),
            #[cfg(feature = "lz4")]
            "lz4+tcp" => parse_host_port(rest).map(|(host, port)| Endpoint::Lz4Tcp { host, port }),
            #[cfg(feature = "zstd")]
            "zstd+tcp" => {
                parse_host_port(rest).map(|(host, port)| Endpoint::ZstdTcp { host, port })
            }
            #[cfg(feature = "ws")]
            "ws" => parse_ws(rest, false),
            #[cfg(feature = "ws")]
            "wss" => parse_ws(rest, true),
            #[cfg(all(feature = "lz4", feature = "ws"))]
            "lz4+ws" => parse_compressed_ws(rest, false, |h, p, pa| Endpoint::Lz4Ws {
                host: h,
                port: p,
                path: pa,
            }),
            #[cfg(all(feature = "lz4", feature = "ws"))]
            "lz4+wss" => parse_compressed_ws(rest, true, |h, p, pa| Endpoint::Lz4Wss {
                host: h,
                port: p,
                path: pa,
            }),
            _ => Err(Error::UnsupportedScheme(scheme.to_string())),
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp { host, port } => write!(f, "tcp://{host}:{port}"),
            Self::Ipc(path) => write!(f, "ipc://{path}"),
            Self::Inproc { name } => write!(f, "inproc://{name}"),
            Self::Udp { group, host, port } => match group {
                Some(g) => write!(f, "udp://{g}@{host}:{port}"),
                None => write!(f, "udp://{host}:{port}"),
            },
            #[cfg(feature = "lz4")]
            Self::Lz4Tcp { host, port } => write!(f, "lz4+tcp://{host}:{port}"),
            #[cfg(feature = "zstd")]
            Self::ZstdTcp { host, port } => write!(f, "zstd+tcp://{host}:{port}"),
            #[cfg(feature = "ws")]
            Self::Ws { host, port, path } => write!(f, "ws://{host}:{port}{path}"),
            #[cfg(feature = "ws")]
            Self::Wss { host, port, path } => write!(f, "wss://{host}:{port}{path}"),
            #[cfg(all(feature = "lz4", feature = "ws"))]
            Self::Lz4Ws { host, port, path } => write!(f, "lz4+ws://{host}:{port}{path}"),
            #[cfg(all(feature = "lz4", feature = "ws"))]
            Self::Lz4Wss { host, port, path } => write!(f, "lz4+wss://{host}:{port}{path}"),
        }
    }
}

impl Endpoint {
    /// Strip the compression scheme prefix so the underlying TCP
    /// transport sees a plain `tcp://` endpoint. Identity for plain
    /// `tcp://`. Returns the endpoint unchanged for `ipc://` /
    /// `inproc://` / `udp://`.
    #[must_use]
    pub fn underlying_tcp(&self) -> Endpoint {
        match self {
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { host, port } => Endpoint::Tcp {
                host: host.clone(),
                port: *port,
            },
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { host, port } => Endpoint::Tcp {
                host: host.clone(),
                port: *port,
            },
            _ => self.clone(),
        }
    }

    /// Re-attach the original endpoint's scheme to a resolved address.
    /// Used after binding through the underlying TCP transport so the
    /// bound endpoint surfaced to the user still says `lz4+tcp://...`.
    #[must_use]
    pub fn rewrap_tcp(&self, resolved: Endpoint) -> Endpoint {
        match (self, resolved) {
            #[cfg(feature = "lz4")]
            (Endpoint::Lz4Tcp { .. }, Endpoint::Tcp { host, port }) => {
                Endpoint::Lz4Tcp { host, port }
            }
            #[cfg(feature = "zstd")]
            (Endpoint::ZstdTcp { .. }, Endpoint::Tcp { host, port }) => {
                Endpoint::ZstdTcp { host, port }
            }
            (_, resolved) => resolved,
        }
    }

    /// Whether this endpoint rides on the TCP byte-stream transport.
    /// Includes the compression-wrapped variants.
    // Not matches!(): #[cfg] attributes are not allowed inside macro patterns.
    pub fn is_tcp_family(&self) -> bool {
        match self {
            Endpoint::Tcp { .. } => true,
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => true,
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => true,
            _ => false,
        }
    }

    /// Strip the compression scheme prefix so the underlying WS
    /// transport sees a plain `ws://` or `wss://` endpoint. Identity
    /// for plain `ws://` / `wss://` and all non-WS endpoints.
    #[cfg(feature = "ws")]
    #[must_use]
    pub fn underlying_ws(&self) -> Endpoint {
        match self {
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Ws { host, port, path } => Endpoint::Ws {
                host: host.clone(),
                port: *port,
                path: path.clone(),
            },
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Wss { host, port, path } => Endpoint::Wss {
                host: host.clone(),
                port: *port,
                path: path.clone(),
            },
            other => other.clone(),
        }
    }

    /// Re-attach the original endpoint's compression scheme to a
    /// resolved WS address. Used after binding so the bound endpoint
    /// surfaced to the user still says `lz4+ws://…` etc.
    #[cfg(feature = "ws")]
    #[must_use]
    pub fn rewrap_ws(&self, resolved: Endpoint) -> Endpoint {
        match (self, resolved) {
            #[cfg(feature = "lz4")]
            (Endpoint::Lz4Ws { .. }, Endpoint::Ws { host, port, path }) => {
                Endpoint::Lz4Ws { host, port, path }
            }
            #[cfg(feature = "lz4")]
            (Endpoint::Lz4Wss { .. }, Endpoint::Wss { host, port, path }) => {
                Endpoint::Lz4Wss { host, port, path }
            }
            (_, resolved) => resolved,
        }
    }

    /// Whether this endpoint uses the WebSocket transport.
    /// Includes the compression-wrapped variants.
    #[cfg(feature = "ws")]
    pub fn is_ws_family(&self) -> bool {
        match self {
            Endpoint::Ws { .. } | Endpoint::Wss { .. } => true,
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Ws { .. } | Endpoint::Lz4Wss { .. } => true,
            _ => false,
        }
    }

    /// Short scheme tag suitable for monitor / log output.
    pub fn scheme(&self) -> &'static str {
        match self {
            Endpoint::Tcp { .. } => "tcp",
            Endpoint::Ipc(_) => "ipc",
            Endpoint::Inproc { .. } => "inproc",
            Endpoint::Udp { .. } => "udp",
            #[cfg(feature = "lz4")]
            Endpoint::Lz4Tcp { .. } => "lz4+tcp",
            #[cfg(feature = "zstd")]
            Endpoint::ZstdTcp { .. } => "zstd+tcp",
            #[cfg(feature = "ws")]
            Endpoint::Ws { .. } => "ws",
            #[cfg(feature = "ws")]
            Endpoint::Wss { .. } => "wss",
            #[cfg(all(feature = "lz4", feature = "ws"))]
            Endpoint::Lz4Ws { .. } => "lz4+ws",
            #[cfg(all(feature = "lz4", feature = "ws"))]
            Endpoint::Lz4Wss { .. } => "lz4+wss",
        }
    }
}

/// Inproc + any non-NULL mechanism makes no sense: both ends are in
/// the same process, the fast path skips the codec entirely. Reject
/// explicitly so the user notices their config doesn't do what they
/// think it does.
pub fn reject_encrypted_inproc(
    endpoint: &Endpoint,
    mechanism: &crate::proto::mechanism::MechanismSetup,
) -> crate::error::Result<()> {
    if matches!(endpoint, Endpoint::Inproc { .. })
        && !matches!(mechanism, crate::proto::mechanism::MechanismSetup::Null)
    {
        return Err(crate::error::Error::InvalidEndpoint(
            "non-NULL mechanisms (PLAIN / CURVE) are not supported on \
             inproc - use ipc:// or tcp:// for authenticated or encrypted channels"
                .into(),
        ));
    }
    Ok(())
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(IpAddr::V4(v4)) => write!(f, "{v4}"),
            Self::Ip(IpAddr::V6(v6)) => write!(f, "[{v6}]"),
            Self::Name(n) => write!(f, "{n}"),
            Self::Wildcard => write!(f, "*"),
        }
    }
}

impl fmt::Display for IpcPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(unix)]
            Self::Filesystem(p) => write!(f, "{}", p.display()),
            #[cfg(unix)]
            Self::Abstract(name) => write!(f, "@{name}"),
            #[cfg(target_os = "windows")]
            Self::NamedPipe(name) => write!(f, "{name}"),
        }
    }
}

impl FromStr for EndpointSpec {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (role, rest) = match s.as_bytes().first() {
            Some(b'@') => (EndpointRole::Bind, &s[1..]),
            Some(b'>') => (EndpointRole::Connect, &s[1..]),
            _ => (EndpointRole::Default, s),
        };
        Ok(Self {
            role,
            endpoint: rest.parse()?,
        })
    }
}

impl fmt::Display for EndpointSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.role {
            EndpointRole::Bind => write!(f, "@{}", self.endpoint),
            EndpointRole::Connect => write!(f, ">{}", self.endpoint),
            EndpointRole::Default => write!(f, "{}", self.endpoint),
        }
    }
}

fn parse_host_port(rest: &str) -> Result<(Host, u16)> {
    // IPv6 must be bracketed: `[::1]:5555`.
    if let Some(stripped) = rest.strip_prefix('[') {
        let close = stripped
            .find(']')
            .ok_or_else(|| Error::InvalidEndpoint(format!("unmatched '[' in {rest}")))?;
        let ip_str = &stripped[..close];
        let after = &stripped[close + 1..];
        let port = after
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .ok_or_else(|| Error::InvalidEndpoint(format!("missing port in {rest}")))?;
        let ip: IpAddr = ip_str
            .parse()
            .map_err(|_| Error::InvalidEndpoint(format!("invalid IPv6 {ip_str}")))?;
        return Ok((Host::Ip(ip), port));
    }

    let (host_str, port_str) = rest
        .rsplit_once(':')
        .ok_or_else(|| Error::InvalidEndpoint(format!("missing port in {rest}")))?;
    let port: u16 = if port_str == "*" {
        0
    } else {
        port_str
            .parse()
            .map_err(|_| Error::InvalidEndpoint(format!("invalid port {port_str}")))?
    };
    let host = parse_host(host_str)?;
    Ok((host, port))
}

fn parse_host(s: &str) -> Result<Host> {
    if s == "*" || s.is_empty() {
        return Ok(Host::Wildcard);
    }
    // Bare IPv6 is ambiguous with host:port -- require bracketed form.
    if s.contains(':') {
        return Err(Error::InvalidEndpoint(format!(
            "IPv6 must be bracketed: [{s}]"
        )));
    }
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Ok(Host::Ip(ip));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
    {
        return Err(Error::InvalidEndpoint(format!("invalid host {s}")));
    }
    Ok(Host::Name(s.to_string()))
}

fn parse_ipc(rest: &str) -> Result<IpcPath> {
    if rest.is_empty() {
        return Err(Error::InvalidEndpoint("empty ipc path".to_string()));
    }

    #[cfg(unix)]
    {
        if let Some(name) = rest.strip_prefix('@') {
            if name.is_empty() {
                return Err(Error::InvalidEndpoint(
                    "empty abstract ipc name".to_string(),
                ));
            }
            return Ok(IpcPath::Abstract(name.to_string()));
        }
        Ok(IpcPath::Filesystem(PathBuf::from(rest)))
    }

    #[cfg(target_os = "windows")]
    {
        validate_ipc_name(rest)?;
        Ok(IpcPath::NamedPipe(rest.to_string()))
    }

    #[cfg(all(not(unix), not(target_os = "windows")))]
    {
        Err(Error::UnsupportedScheme(
            "IPC is not supported on this platform".into(),
        ))
    }
}

/// Validate a Windows named pipe name.
/// - Must be 1-256 characters
/// - Cannot be empty
/// - Cannot contain reserved device names (CON, PRN, AUX, NUL, COM1-9, LPT1-9)
/// - Cannot contain control characters or invalid NTFS characters (<>:"|?*)
#[cfg(target_os = "windows")]
pub fn validate_ipc_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 256 {
        return Err(Error::InvalidEndpoint(format!(
            "Windows IPC name must be 1-256 chars; got {} chars",
            name.len()
        )));
    }

    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];

    let upper = name.to_uppercase();
    if reserved.iter().any(|r| r.eq_ignore_ascii_case(&upper)) {
        return Err(Error::InvalidEndpoint(format!(
            "'{name}' is a reserved Windows device name"
        )));
    }

    if name
        .chars()
        .any(|c| c.is_control() || r#"<>:"|?*"#.contains(c))
    {
        return Err(Error::InvalidEndpoint(
            "IPC name contains invalid characters; must not contain: < > : \" | ? *".into(),
        ));
    }

    Ok(())
}

#[cfg(feature = "ws")]
fn parse_ws(rest: &str, tls: bool) -> Result<Endpoint> {
    let (hp, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = parse_host_port(hp)?;
    if path.bytes().any(|b| matches!(b, b'\r' | b'\n')) {
        return Err(Error::InvalidEndpoint("invalid ws path".into()));
    }
    let path = path.to_string();
    if tls {
        Ok(Endpoint::Wss { host, port, path })
    } else {
        Ok(Endpoint::Ws { host, port, path })
    }
}

#[cfg(all(feature = "lz4", feature = "ws"))]
fn parse_compressed_ws(
    rest: &str,
    tls: bool,
    wrap: impl FnOnce(Host, u16, String) -> Endpoint,
) -> Result<Endpoint> {
    match parse_ws(rest, tls)? {
        Endpoint::Ws { host, port, path } | Endpoint::Wss { host, port, path } => {
            Ok(wrap(host, port, path))
        }
        _ => unreachable!(),
    }
}

fn parse_udp(rest: &str) -> Result<Endpoint> {
    let (group, hp) = match rest.split_once('@') {
        Some((g, hp)) if !g.is_empty() => (Some(g.to_string()), hp),
        _ => (None, rest),
    };
    let (host, port) = parse_host_port(hp)?;
    Ok(Endpoint::Udp { group, host, port })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_endpoint_parses() {
        let ep: Endpoint = "tcp://localhost:5555".parse().unwrap();
        assert_eq!(ep.scheme(), "tcp");
        assert!(ep.is_tcp_family());
    }

    #[test]
    fn inproc_endpoint_parses() {
        let ep: Endpoint = "inproc://service".parse().unwrap();
        assert_eq!(ep.scheme(), "inproc");
        assert!(matches!(ep, Endpoint::Inproc { name } if name == "service"));
    }

    #[test]
    fn endpoint_display_roundtrip() {
        let endpoints = vec![
            "tcp://localhost:5555",
            "inproc://test",
            "udp://localhost:5555",
        ];

        for ep_str in endpoints {
            let ep: Endpoint = ep_str.parse().unwrap();
            assert_eq!(ep.to_string(), ep_str);
        }
    }

    #[test]
    #[cfg(unix)]
    fn unix_ipc_filesystem() {
        let ep: Endpoint = "ipc:///tmp/socket".parse().unwrap();
        assert_eq!(ep.scheme(), "ipc");
        let ep_str = ep.to_string();
        assert!(ep_str.starts_with("ipc://"));
    }

    #[test]
    #[cfg(unix)]
    fn unix_ipc_abstract_namespace() {
        let ep: Endpoint = "ipc://@myservice".parse().unwrap();
        assert_eq!(ep.scheme(), "ipc");
        assert_eq!(ep.to_string(), "ipc://@myservice");
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn windows_ipc_named_pipe() {
        let ep: Endpoint = "ipc://myservice".parse().unwrap();
        assert_eq!(ep.scheme(), "ipc");
        assert_eq!(ep.to_string(), "ipc://myservice");
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn windows_ipc_validates_reserved_names() {
        // Reserved names should be rejected
        let reserved = ["CON", "PRN", "AUX", "NUL", "COM1", "LPT1"];
        for name in &reserved {
            let result: Result<Endpoint> = format!("ipc://{name}").parse();
            assert!(result.is_err(), "Reserved name '{name}' should be rejected");
        }
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn windows_ipc_validates_invalid_chars() {
        let invalid_names = vec!["my<pipe", "my|pipe", "my:pipe", "my?pipe", "my*pipe"];
        for name in invalid_names {
            let result: Result<Endpoint> = format!("ipc://{name}").parse();
            assert!(
                result.is_err(),
                "Name '{name}' with invalid chars should be rejected"
            );
        }
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn windows_ipc_valid_names() {
        let valid_names = vec!["myservice", "Service123", "my_service", "my-service"];
        for name in valid_names {
            let ep: Endpoint = format!("ipc://{name}").parse().unwrap();
            assert_eq!(ep.scheme(), "ipc");
        }
    }

    #[test]
    fn endpoint_spec_with_bind_role() {
        let spec: EndpointSpec = "@tcp://localhost:5555".parse().unwrap();
        assert_eq!(spec.role, EndpointRole::Bind);
        assert_eq!(spec.to_string(), "@tcp://localhost:5555");
    }

    #[test]
    fn endpoint_spec_with_connect_role() {
        let spec: EndpointSpec = ">tcp://localhost:5555".parse().unwrap();
        assert_eq!(spec.role, EndpointRole::Connect);
        assert_eq!(spec.to_string(), ">tcp://localhost:5555");
    }

    #[test]
    fn endpoint_spec_with_default_role() {
        let spec: EndpointSpec = "tcp://localhost:5555".parse().unwrap();
        assert_eq!(spec.role, EndpointRole::Default);
        assert_eq!(spec.to_string(), "tcp://localhost:5555");
    }

    #[test]
    fn invalid_endpoint_missing_scheme() {
        let result: Result<Endpoint> = "localhost:5555".parse();
        assert!(result.is_err());
    }

    #[test]
    fn invalid_endpoint_unknown_scheme() {
        let result: Result<Endpoint> = "unknown://localhost:5555".parse();
        assert!(result.is_err());
    }
}
