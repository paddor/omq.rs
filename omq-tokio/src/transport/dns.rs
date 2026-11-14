//! Bounded, runtime-independent DNS resolution.
//!
//! Platform `getaddrinfo` can block after its future is canceled. Keep it on
//! a fixed number of OS workers with a bounded queue; dropping the caller
//! releases its future while an active OS lookup keeps occupying its worker.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, LazyLock, Mutex};

use futures::channel::oneshot;

use omq_proto::endpoint::Host;
use omq_proto::{Error, Result};

/// How long one address may stay unanswered before the next one is dialed
/// alongside it (RFC 8305, Connection Attempt Delay).
const NEXT_ADDRESS_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

/// Run `attempt` on `addrs` in order and return the first success.
///
/// A failed address moves on to the next at once. An address that does not
/// answer keeps its attempt, and the next one starts after
/// [`NEXT_ADDRESS_DELAY`]: a silent host would otherwise use up the
/// caller's whole deadline, on this attempt and on every retry. Attempts
/// still pending at the first success are dropped. The error is the last
/// failure, or `None` without addresses.
pub(crate) async fn first_connected<T, E, F>(
    addrs: Vec<SocketAddr>,
    mut attempt: impl FnMut(SocketAddr) -> F,
) -> std::result::Result<T, Option<E>>
where
    F: Future<Output = std::result::Result<T, E>>,
{
    use futures::StreamExt;

    let mut remaining = addrs.into_iter();
    let mut attempts = futures::stream::FuturesUnordered::new();
    let mut last_err = None;
    loop {
        if attempts.is_empty() {
            let Some(addr) = remaining.next() else {
                break;
            };
            attempts.push(attempt(addr));
        }
        tokio::select! {
            Some(result) = attempts.next() => match result {
                Ok(connected) => return Ok(connected),
                Err(e) => last_err = Some(e),
            },
            () = tokio::time::sleep(NEXT_ADDRESS_DELAY), if remaining.len() != 0 => {
                if let Some(addr) = remaining.next() {
                    attempts.push(attempt(addr));
                }
            }
        }
    }
    Err(last_err)
}

const WORKERS: usize = 4;
const QUEUED: usize = 8;
const MAX_ADDRESSES: usize = 256;
const MAX_HOST_BYTES: usize = 255;

static GLOBAL: LazyLock<io::Result<Resolver>> = LazyLock::new(|| {
    let lookup: Arc<Lookup> = Arc::new(system_lookup);
    Resolver::new(WORKERS, QUEUED, &lookup)
});

#[cfg(test)]
static STALLED_TEST_LOOKUPS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

type Lookup = dyn Fn(&str, u16) -> io::Result<Vec<SocketAddr>> + Send + Sync;

struct Request {
    host: String,
    port: u16,
    reply: oneshot::Sender<io::Result<Vec<SocketAddr>>>,
}

struct Resolver {
    sender: SyncSender<Request>,
}

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Resolver")
    }
}

impl Resolver {
    fn new(workers: usize, queued: usize, lookup: &Arc<Lookup>) -> io::Result<Self> {
        let (sender, receiver) = sync_channel::<Request>(queued);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut started = 0;

        for index in 0..workers {
            let receiver = receiver.clone();
            let lookup = lookup.clone();

            if std::thread::Builder::new()
                .name(format!("omq-dns-{index}"))
                .spawn(move || {
                    loop {
                        let Ok(job) = receiver.lock().expect("DNS receiver poisoned").recv() else {
                            break;
                        };

                        if !job.reply.is_canceled() {
                            let result = lookup(&job.host, job.port);
                            let _ = job.reply.send(result);
                        }
                    }
                })
                .is_ok()
            {
                started += 1;
            }
        }

        if started == 0 {
            return Err(io::Error::other("failed to start DNS resolver workers"));
        }

        Ok(Self { sender })
    }

    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>> {
        if host.is_empty() || host.len() > MAX_HOST_BYTES || host.as_bytes().contains(&0) {
            return Err(Error::InvalidEndpoint("invalid DNS hostname".into()));
        }

        let (reply, result) = oneshot::channel();
        self.sender
            .try_send(Request {
                host: host.into(),
                port,
                reply,
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => Error::Io(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "DNS resolver queue full",
                )),
                TrySendError::Disconnected(_) => Error::Io(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "DNS resolver unavailable",
                )),
            })?;

        result
            .await
            .map_err(|_| {
                Error::Io(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "DNS resolver worker stopped",
                ))
            })?
            .map_err(Error::Io)
    }
}

fn system_lookup(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    let addresses: Vec<_> = (host, port)
        .to_socket_addrs()?
        .take(MAX_ADDRESSES + 1)
        .collect();

    if addresses.len() > MAX_ADDRESSES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "too many DNS addresses",
        ));
    }

    if addresses.is_empty() {
        return Err(io::Error::other(format!("no addresses for {host}:{port}")));
    }

    Ok(addresses)
}

#[cfg(all(test, any(feature = "ws", feature = "quic")))]
pub(crate) fn stalled_test_lookups() -> usize {
    STALLED_TEST_LOOKUPS.load(std::sync::atomic::Ordering::Acquire)
}

pub(crate) async fn resolve(host: &str, port: u16) -> Result<Vec<SocketAddr>> {
    #[cfg(test)]
    if host == "omq-test-stall.invalid" {
        STALLED_TEST_LOOKUPS.fetch_add(1, std::sync::atomic::Ordering::Release);
        return std::future::pending().await;
    }

    #[cfg(test)]
    if host == "omq-test-lookup.invalid" {
        return test_lookup::resolve(port);
    }

    match &*GLOBAL {
        Ok(resolver) => resolver.resolve(host, port).await,
        Err(error) => Err(Error::Io(io::Error::other(error.to_string()))),
    }
}

/// Address to bind: the IPv4 wildcard, a literal IP, or the first DNS result.
pub(crate) async fn bind_addr(host: &Host, port: u16) -> Result<SocketAddr> {
    match host {
        Host::Wildcard => Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)),
        Host::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
        Host::Name(name) => Ok(resolve_name(name, port).await?[0]),
        _ => unreachable!(),
    }
}

/// Addresses to dial, in resolver order. Never empty.
pub(crate) async fn connect_addrs(host: &Host, port: u16) -> Result<Vec<SocketAddr>> {
    match host {
        Host::Wildcard => Err(Error::InvalidEndpoint(
            "cannot connect to wildcard host".into(),
        )),
        Host::Ip(ip) => Ok(vec![SocketAddr::new(*ip, port)]),
        Host::Name(name) => resolve_name(name, port).await,
        _ => unreachable!(),
    }
}

/// Addresses for a hostname, in resolver order. Never empty.
pub(crate) async fn resolve_name(name: &str, port: u16) -> Result<Vec<SocketAddr>> {
    let addrs = resolve(name, port).await?;
    if addrs.is_empty() {
        return Err(Error::Io(io::Error::other(format!(
            "no addresses for {name}:{port}"
        ))));
    }

    Ok(addrs)
}

#[cfg(test)]
pub(crate) mod test_lookup;

#[cfg(test)]
mod tests;
