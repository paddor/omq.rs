use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) async fn stream_worker(
    context: Context,
    deadline: Instant,
    seed: u64,
    counts: Arc<Counts>,
) {
    let mut sequence = 0;
    while Instant::now() < deadline {
        let stream = context.socket(SocketType::Stream, options());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint: Endpoint = format!("tcp://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        stream.connect(endpoint).await.unwrap();
        let (remote, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = remote.into_split();
        let greeting = tokio::time::timeout(IO_TIMEOUT, stream.recv())
            .await
            .unwrap()
            .unwrap();
        let identity = greeting.part_bytes(0).unwrap();
        assert_eq!(greeting.part_slice(1), Some(&[][..]));
        counts.connections.fetch_add(1, Ordering::Relaxed);
        for _ in 0..16 {
            let body = payload(seed, sequence, SIZES[sequence as usize % SIZES.len()]);
            let progress = [AtomicU64::new(0), AtomicU64::new(0)];
            let mut events = stream.monitor();
            tokio::time::timeout(IO_TIMEOUT, async {
                let outgoing = async {
                    stream
                        .send(Message::multipart([identity.clone(), body.clone()]))
                        .await
                        .unwrap();
                    let mut received = vec![0; body.len()];
                    let mut offset = 0;
                    while offset < received.len() {
                        let n = reader.read(&mut received[offset..]).await.unwrap();
                        assert!(n != 0, "STREAM disconnected under pacing");
                        offset += n;
                        progress[0].store(offset as u64, Ordering::Relaxed);
                    }
                    assert_eq!(&received[..], body.as_ref());
                    counts.received(body.len());
                };
                let incoming = async {
                    writer.write_all(&body).await.unwrap();
                    let mut offset = 0;
                    while offset < body.len() {
                        let message = stream.recv().await.unwrap();
                        assert_eq!(message.len(), 2);
                        assert_eq!(message.part_bytes(0).unwrap(), identity);
                        let chunk = message.part_slice(1).unwrap();
                        assert!(!chunk.is_empty(), "STREAM disconnected under pacing");
                        assert!(offset + chunk.len() <= body.len());
                        assert_eq!(chunk, &body[offset..offset + chunk.len()]);
                        offset += chunk.len();
                        progress[1].store(offset as u64, Ordering::Relaxed);
                    }
                    counts.received(body.len());
                };
                tokio::join!(outgoing, incoming);
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "STREAM sequence={sequence} size={} egress={} ingress={} events={:?}",
                    body.len(),
                    progress[0].load(Ordering::Relaxed),
                    progress[1].load(Ordering::Relaxed),
                    recent_events(&mut events)
                )
            });
            sequence += 1;
            if Instant::now() >= deadline {
                break;
            }
        }
        close(stream).await;
    }
    assert!(sequence != 0, "STREAM made no progress");
}

async fn udp_pair(sender: &Context, receiver: &Context) -> (Socket, Socket) {
    let dish = receiver.socket(SocketType::Dish, options());
    dish.join("s").await.unwrap();
    let endpoint = dish
        .bind("udp://127.0.0.1:0".parse::<Endpoint>().unwrap())
        .await
        .unwrap();
    let radio = sender.socket(SocketType::Radio, options());
    radio.connect(endpoint).await.unwrap();
    (radio, dish)
}

pub(super) async fn udp_worker(
    limited: Context,
    remote: Context,
    deadline: Instant,
    seed: u64,
    counts: Arc<Counts>,
) {
    let mut sequence = 0;
    while Instant::now() < deadline {
        let (out, out_receiver) = udp_pair(&limited, &remote).await;
        let (input, in_receiver) = udp_pair(&remote, &limited).await;
        counts.connections.fetch_add(2, Ordering::Relaxed);
        for _ in 0..16 {
            let body = payload(
                seed,
                sequence,
                SIZES[sequence as usize % SIZES.len()].min(16384),
            );
            // One datagram outstanding in each direction avoids overflowing
            // UDP's lossy receive queues while their reads are paced.
            let transfer = async |sender: &Socket, receiver: &Socket, body: Bytes| {
                sender
                    .send(Message::multipart([Bytes::from_static(b"s"), body.clone()]))
                    .await
                    .unwrap();
                let message = receiver.recv().await.unwrap();
                assert_eq!(message.len(), 2);
                assert_eq!(message.part_slice(0), Some(&b"s"[..]));
                assert_eq!(message.part_slice(1), Some(body.as_ref()));
                counts.received(body.len());
            };
            tokio::time::timeout(IO_TIMEOUT, async {
                tokio::join!(
                    transfer(&out, &out_receiver, body.clone()),
                    transfer(&input, &in_receiver, body)
                );
            })
            .await
            .expect("UDP made no progress under mixed demand");
            sequence += 1;
            if Instant::now() >= deadline {
                break;
            }
        }
        for socket in [out, out_receiver, input, in_receiver] {
            close(socket).await;
        }
    }
    assert!(sequence != 0, "UDP made no progress");
}
