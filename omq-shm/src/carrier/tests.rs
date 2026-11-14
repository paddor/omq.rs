use super::*;
use crate::mapping::LEASES;
use std::io::Read;
use std::sync::atomic::Ordering;

fn pair() -> (Carrier, Mapping, Carrier, Mapping) {
    let sender = Carrier::new(2, true).unwrap();
    let receiver = Carrier::new(2, false).unwrap();

    for signal in [&sender.reader, &sender.writer] {
        receiver
            .accept_signal(signal.get().unwrap().try_clone().unwrap())
            .unwrap();
    }

    let outgoing = Mapping::create(Carrier::length(2), LEASES).unwrap();
    let incoming = Mapping::import(outgoing.descriptor().unwrap()).unwrap();
    (sender, outgoing, receiver, incoming)
}

fn counter(file: &File) -> io::Result<u64> {
    let mut bytes = [0; std::mem::size_of::<u64>()];
    (&*file).read_exact(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}

#[test]
fn notifiers_must_be_nonblocking_eventfds() {
    use rustix::fs::OFlags;
    let carrier = Carrier::new(2, false).unwrap();
    let epoll = File::from(
        rustix::event::epoll::create(rustix::event::epoll::CreateFlags::CLOEXEC).unwrap(),
    );
    rustix::fs::fcntl_setfl(&epoll, OFlags::NONBLOCK).unwrap();
    let blocking =
        File::from(rustix::event::eventfd(0, rustix::event::EventfdFlags::CLOEXEC).unwrap());
    let (pipe, _writer) = rustix::pipe::pipe_with(rustix::pipe::PipeFlags::NONBLOCK).unwrap();
    let memfd = File::from(
        rustix::fs::memfd_create("omq-notifier", rustix::fs::MemfdFlags::CLOEXEC).unwrap(),
    );

    for file in [epoll, blocking, File::from(pipe), memfd] {
        assert_eq!(
            carrier.accept_signal(file).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    let notifier = eventfd().unwrap();
    assert!(
        carrier
            .accept_signal(notifier.try_clone().unwrap())
            .unwrap()
    );
    assert_eq!(
        counter(&notifier).unwrap_err().kind(),
        io::ErrorKind::WouldBlock,
        "the probe must not signal"
    );
}

#[test]
fn wraps_and_reuses_only_returned_bytes() {
    let (sender, outgoing, receiver, incoming) = pair();
    let first = vec![1; CAPACITY - 7];
    assert_eq!(
        sender.write(&outgoing, &[IoSlice::new(&first)]).unwrap(),
        first.len()
    );

    let mut output = vec![0; CAPACITY - 11];
    assert_eq!(receiver.read(&incoming, &mut output).unwrap(), output.len());
    assert!(output.iter().all(|&byte| byte == 1));

    let second = vec![2; CAPACITY];
    assert_eq!(
        sender.write(&outgoing, &[IoSlice::new(&second)]).unwrap(),
        CAPACITY - 4
    );
    assert_eq!(
        sender
            .write(&outgoing, &[IoSlice::new(b"full")])
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );

    output.resize(CAPACITY, 0);
    assert_eq!(receiver.read(&incoming, &mut output).unwrap(), CAPACITY);
    assert_eq!(&output[..4], &[1; 4]);
    assert!(output[4..].iter().all(|&byte| byte == 2));
    assert_eq!(
        receiver.read(&incoming, &mut output).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn notifies_only_armed_consumers_and_producers() {
    let (sender, outgoing, receiver, incoming) = pair();
    assert!(!receiver.arm_reader(&incoming, false).unwrap());
    sender.write(&outgoing, &[IoSlice::new(b"one")]).unwrap();
    sender.write(&outgoing, &[IoSlice::new(b"two")]).unwrap();
    assert_eq!(counter(receiver.reader.get().unwrap()).unwrap(), 1);

    sender.park_writer(&outgoing);
    let mut output = [0; 6];
    assert_eq!(receiver.read(&incoming, &mut output).unwrap(), 6);
    assert_eq!(&output, b"onetwo");
    assert_eq!(counter(sender.writer.get().unwrap()).unwrap(), 1);

    assert!(!receiver.arm_reader(&incoming, true).unwrap());
    sender
        .write(&outgoing, &[IoSlice::new(b"polling")])
        .unwrap();
    assert_eq!(
        counter(receiver.reader.get().unwrap()).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );

    // Arm after publication must see data without relying on a wake.
    assert!(receiver.arm_reader(&incoming, false).unwrap());
}

#[test]
fn registration_acknowledgment_closes_both_arm_races() {
    let (sender, outgoing, receiver, incoming) = pair();
    receiver.acknowledge_imports(&incoming, 3).unwrap();
    sender.park_writer(&outgoing);
    assert_eq!(sender.imported(&outgoing), 3);
    receiver.acknowledge_imports(&incoming, 4).unwrap();
    assert_eq!(sender.imported(&outgoing), 4);
    assert_eq!(counter(sender.writer.get().unwrap()).unwrap(), 1);
}

#[test]
fn rejects_corrupt_cursors_and_handles_empty_buffers() {
    let (sender, outgoing, receiver, incoming) = pair();
    assert_eq!(sender.write(&outgoing, &[]).unwrap(), 0);
    assert_eq!(receiver.read(&incoming, &mut []).unwrap(), 0);
    outgoing
        .atomic64(sender.offset + WRITE)
        .store(CAPACITY as u64 + 1, Ordering::Release);
    assert_eq!(
        receiver.read(&incoming, &mut [0]).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        sender
            .write(&outgoing, &[IoSlice::new(b"bad")])
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn leading_empty_slices_do_not_report_a_zero_write() {
    let (sender, outgoing, receiver, incoming) = pair();
    let mut slices = vec![IoSlice::new(&[]); MAX_SLICES + 1];
    slices.push(IoSlice::new(b"data"));
    assert_eq!(sender.write(&outgoing, &slices).unwrap(), 4);
    let mut output = [0; 4];
    assert_eq!(receiver.read(&incoming, &mut output).unwrap(), 4);
    assert_eq!(&output, b"data");
}

#[test]
fn cursor_overflow_preserves_order() {
    let (sender, outgoing, receiver, incoming) = pair();
    let cursor = u64::MAX - 3;
    outgoing
        .atomic64(sender.offset + WRITE)
        .store(cursor, Ordering::Relaxed);
    outgoing
        .atomic64(sender.offset + READ)
        .store(cursor, Ordering::Relaxed);

    sender
        .write(&outgoing, &[IoSlice::new(b"overflow")])
        .unwrap();
    let mut output = [0; 8];
    assert_eq!(receiver.read(&incoming, &mut output).unwrap(), output.len());
    assert_eq!(&output, b"overflow");
}

#[test]
fn concurrent_partial_transfers_preserve_publication_order() {
    let (sender, outgoing, receiver, incoming) = pair();
    let bytes: Vec<_> = (0..2 * CAPACITY + 937)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut offset = 0;
            while offset < bytes.len() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "carrier writer stalled"
                );
                sender.park_writer(&outgoing);
                let end = (offset + 97).min(bytes.len());

                match sender.write(&outgoing, &[IoSlice::new(&bytes[offset..end])]) {
                    Ok(length) => offset += length,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::yield_now();
                    }
                    Err(error) => panic!("carrier write: {error}"),
                }
            }
        });

        let mut offset = 0;
        let mut output = [0; 257];

        while offset < bytes.len() {
            assert!(
                std::time::Instant::now() < deadline,
                "carrier reader stalled"
            );
            receiver.arm_reader(&incoming, false).unwrap();

            match receiver.read(&incoming, &mut output) {
                Ok(length) => {
                    assert_eq!(&output[..length], &bytes[offset..offset + length]);
                    offset += length;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(error) => panic!("carrier read: {error}"),
            }
        }
    });
}
