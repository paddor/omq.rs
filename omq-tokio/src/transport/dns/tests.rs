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
