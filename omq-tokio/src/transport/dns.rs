//! Bounded, runtime-independent DNS resolution.
//!
//! Platform `getaddrinfo` can block after its future is canceled. Keep it on
//! a fixed number of OS workers with a bounded queue; dropping the caller
//! releases its future while an active OS lookup keeps occupying its worker.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, LazyLock, Mutex};

use futures::channel::oneshot;

use omq_proto::{Error, Result};

const WORKERS: usize = 4;
const QUEUED: usize = 8;
const MAX_ADDRESSES: usize = 256;
const MAX_HOST_BYTES: usize = 255;

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

static GLOBAL: LazyLock<io::Result<Resolver>> = LazyLock::new(|| {
    let lookup: Arc<Lookup> = Arc::new(system_lookup);
    Resolver::new(WORKERS, QUEUED, &lookup)
});

#[cfg(test)]
static STALLED_TEST_LOOKUPS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

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

#[cfg(test)]
pub(crate) mod test_lookup {
    use super::*;
    use std::collections::HashMap;
    use std::net::IpAddr;

    struct Answer {
        address: Option<IpAddr>,
        calls: usize,
    }

    static ANSWERS: LazyLock<Mutex<HashMap<u16, Answer>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    #[derive(Debug)]
    pub(crate) struct ScopedLookup {
        port: u16,
    }

    impl ScopedLookup {
        pub(crate) fn new(port: u16, address: Option<IpAddr>) -> Self {
            let previous = ANSWERS
                .lock()
                .unwrap()
                .insert(port, Answer { address, calls: 0 });
            assert!(previous.is_none(), "test DNS port already in use");
            Self { port }
        }

        pub(crate) fn set_address(&self, address: Option<IpAddr>) {
            ANSWERS.lock().unwrap().get_mut(&self.port).unwrap().address = address;
        }

        pub(crate) fn calls(&self) -> usize {
            ANSWERS.lock().unwrap().get(&self.port).unwrap().calls
        }
    }

    impl Drop for ScopedLookup {
        fn drop(&mut self) {
            ANSWERS.lock().unwrap().remove(&self.port);
        }
    }

    pub(super) fn resolve(port: u16) -> Result<Vec<SocketAddr>> {
        let mut answers = ANSWERS.lock().unwrap();
        let answer = answers.get_mut(&port).expect("test DNS answer missing");
        answer.calls += 1;
        match answer.address {
            Some(address) => Ok(vec![SocketAddr::new(address, port)]),
            None => Err(Error::Io(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "test DNS failure",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Condvar;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn bounded_workers_and_queue_survive_canceled_lookup() {
        let started = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let lookup: Arc<Lookup> = {
            let started = started.clone();
            let gate = gate.clone();
            Arc::new(move |_, port| {
                started.fetch_add(1, Ordering::Release);
                let (lock, signal) = &*gate;
                let mut ready = lock.lock().unwrap();
                while !*ready {
                    ready = signal.wait(ready).unwrap();
                }
                Ok(vec![SocketAddr::from(([127, 0, 0, 1], port))])
            })
        };
        let resolver = Arc::new(Resolver::new(1, 1, &lookup).unwrap());
        let first = {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.resolve("localhost", 1234).await })
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while started.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = {
            let resolver = resolver.clone();
            tokio::spawn(async move { resolver.resolve("localhost", 1235).await })
        };
        tokio::task::yield_now().await;
        let full = resolver.resolve("localhost", 1236).await.unwrap_err();
        assert!(matches!(full, Error::Io(ref io) if io.kind() == io::ErrorKind::WouldBlock));
        first.abort();
        assert_eq!(started.load(Ordering::Acquire), 1);
        {
            let (lock, signal) = &*gate;
            *lock.lock().unwrap() = true;
            signal.notify_all();
        }
        assert_eq!(
            second.await.unwrap().unwrap(),
            vec![SocketAddr::from(([127, 0, 0, 1], 1235))]
        );
        assert_eq!(started.load(Ordering::Acquire), 2);
    }

    #[test]
    fn empty_excessive_and_embedded_nul_names_fail_before_dispatch() {
        let lookup: Arc<Lookup> = Arc::new(system_lookup);
        let resolver = Resolver::new(1, 1, &lookup).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for name in ["", "a\\0b", &"a".repeat(MAX_HOST_BYTES + 1)] {
            assert!(runtime.block_on(resolver.resolve(name, 443)).is_err());
        }
    }
}
