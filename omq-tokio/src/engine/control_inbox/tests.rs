use super::*;
use omq_proto::proto::Command;
use std::time::{Duration, Instant};

fn command(value: u8) -> PeerDriverCommand {
    PeerDriverCommand::SendCommand(Command::Subscribe(bytes::Bytes::from(vec![value])))
}

#[tokio::test]
async fn activation_and_close_bypass_full_protocol_lane() {
    let (sender, mut receiver) = channel(1);
    sender.try_send(command(1)).unwrap();
    sender.try_send(command(2)).unwrap();
    sender
        .try_send(PeerDriverCommand::ActivateDataPlane)
        .unwrap();

    assert!(matches!(
        receiver.recv_lifecycle().await,
        Some(PeerDriverCommand::ActivateDataPlane)
    ));

    sender.try_send(PeerDriverCommand::Close).unwrap();
    assert!(matches!(
        sender.try_send(command(3)),
        Err(mpsc::error::TrySendError::Closed(_))
    ));
    assert!(matches!(
        receiver.recv_lifecycle().await,
        Some(PeerDriverCommand::Close)
    ));
}

#[tokio::test]
async fn graceful_close_retains_accepted_protocol_fifo() {
    let (sender, mut receiver) = channel(1);
    sender.try_send(command(1)).unwrap();
    sender.try_send(command(2)).unwrap();
    sender
        .try_send(PeerDriverCommand::DrainAndClose {
            deadline: Some(Instant::now() + Duration::from_secs(1)),
        })
        .unwrap();
    assert!(matches!(
        receiver.recv().await,
        Some(PeerDriverCommand::DrainAndClose { .. })
    ));
    assert!(!receiver.is_empty());

    for expected in [1, 2] {
        let Some(PeerDriverCommand::SendCommand(Command::Subscribe(prefix))) =
            receiver.recv().await
        else {
            panic!("protocol command");
        };

        assert_eq!(prefix.as_ref(), &[expected]);
    }

    assert!(receiver.is_empty());
}

#[tokio::test]
async fn protocol_commands_never_wait_and_keep_fifo_across_the_ring() {
    let (sender, mut receiver) = channel(2);
    for value in 0..5 {
        tokio::time::timeout(Duration::from_secs(1), sender.send(command(value)))
            .await
            .unwrap()
            .unwrap();
    }

    // A command sent after a partial drain still follows the overflow.
    let first = receiver.try_recv().unwrap();
    sender.try_send(command(5)).unwrap();
    let mut drained = vec![first];

    while let Ok(command) = receiver.try_recv() {
        drained.push(command);
    }

    let prefixes: Vec<u8> = drained
        .into_iter()
        .map(|command| {
            let PeerDriverCommand::SendCommand(Command::Subscribe(prefix)) = command else {
                panic!("protocol command");
            };

            prefix[0]
        })
        .collect();

    assert_eq!(prefixes, [0, 1, 2, 3, 4, 5]);
    assert!(receiver.is_empty());
}

/// The ring holds older commands than the overflow. A receive that saw
/// the ring empty must not then take from the overflow past commands
/// the sender put in the ring meanwhile.
#[test]
fn overflowed_commands_never_overtake_the_ring() {
    const COMMANDS: u32 = 300_000;
    let (sender, mut receiver) = channel(1);
    let producer = std::thread::spawn(move || {
        for value in 0..COMMANDS {
            let prefix = bytes::Bytes::copy_from_slice(&value.to_be_bytes());
            sender
                .try_send(PeerDriverCommand::SendCommand(Command::Subscribe(prefix)))
                .unwrap();
        }
        sender
    });
    let mut next = 0;
    while next < COMMANDS {
        let Ok(command) = receiver.try_recv() else {
            std::hint::spin_loop();
            continue;
        };
        let PeerDriverCommand::SendCommand(Command::Subscribe(prefix)) = command else {
            panic!("unexpected command");
        };
        let value = u32::from_be_bytes(prefix[..].try_into().unwrap());
        assert_eq!(value, next, "a command overtook an older one");
        next += 1;
    }
    drop(producer.join().unwrap());
}
