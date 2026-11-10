use super::*;
use futures::FutureExt as _;

#[tokio::test]
async fn a_full_driver_lane_cannot_take_another_drivers_capacity_or_starve_it() {
    let (registrar, mut receive): (DataSender, DataReceiver) =
        fanring::mpsc::channel_with_policy(256);
    let mut busy = PeerOutput::actor(registrar.try_register().unwrap());
    let mut cold = PeerOutput::actor(registrar.try_register().unwrap());

    for index in 0..256_u32 {
        busy.try_send(1, Message::single(index.to_le_bytes().to_vec()), false)
            .unwrap();
    }

    assert!(matches!(
        busy.try_send(1, Message::single("full"), false),
        Err(SendPipeError::Full(_))
    ));
    cold.try_send(2, Message::single("cold"), false).unwrap();
    let mut next = 0_u32;

    loop {
        let data = receive.recv_async().await.unwrap();
        if data.peer_id == 2 {
            break;
        }

        assert_eq!(
            data.message.part_slice(0),
            Some(next.to_le_bytes().as_slice())
        );
        busy.try_send(
            1,
            Message::single((256 + next).to_le_bytes().to_vec()),
            false,
        )
        .unwrap();
        next += 1;
        assert!(
            next < 256,
            "continuous producer starved an already ready driver lane"
        );
    }
}

#[tokio::test]
async fn cancelled_capacity_wait_keeps_release_and_teardown_wakes() {
    let (sender, mut receive): (DataSender, DataReceiver) = fanring::mpsc::channel_with_policy(1);
    let mut output = PeerOutput::actor(sender);

    output.try_send(1, Message::single("first"), false).unwrap();
    assert!(output.ready().now_or_never().is_none());

    receive.recv_async().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), output.ready())
        .await
        .unwrap()
        .unwrap();

    output.try_send(1, Message::single("last"), false).unwrap();
    assert!(output.ready().now_or_never().is_none());

    drop(receive);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), output.ready())
            .await
            .unwrap()
            .is_err()
    );
    assert!(matches!(
        output.try_send(1, Message::single("closed"), false),
        Err(SendPipeError::Closed(_))
    ));
}
