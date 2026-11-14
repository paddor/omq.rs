use super::*;
use crate::engine::send_pipe;
use omq_proto::message::Message;

#[test]
fn dropped_lease_returns_the_consumer_with_its_messages() {
    let (mut tx, rx) = send_pipe(4);
    let home = SendPipeHome::new(rx);
    let lease = home.lend().expect("consumer at home");
    assert!(home.lend().is_none(), "a lent consumer was lent twice");

    tx.try_send(Message::single("queued")).unwrap();
    drop(lease);

    let rx = home.take().expect("the lease did not return the consumer");
    assert!(!rx.is_empty());
    assert!(!rx.is_disconnected());
}

#[test]
fn lease_without_a_home_closes_the_pipe() {
    let (mut tx, rx) = send_pipe(4);
    let home = SendPipeHome::new(rx);
    let lease = home.lend().unwrap();
    drop(home);
    drop(lease);

    assert!(tx.try_send(Message::single("late")).is_err());
}

#[test]
fn detached_consumer_leaves_its_home_empty() {
    let (_tx, rx) = send_pipe(4);
    let home = SendPipeHome::new(rx);
    let consumer = home.lend().unwrap().detach();

    assert!(consumer.is_some());
    assert!(home.take().is_none());
}
