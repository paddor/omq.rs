//! Ready receives spend the Tokio task budget, so a loop over a socket that
//! stays ready lets the other tasks on its worker run.
mod test_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use omq_tokio::{Message, Options, Socket, SocketType};

const DEADLINE: Duration = Duration::from_secs(5);

/// More ready receives than a task's budget, fewer messages than the queues
/// hold.
const OPERATIONS: usize = 200;

fn message() -> Message {
    Message::single(Bytes::from_static(b"m"))
}

fn options() -> Options {
    Options::default().send_hwm(1000).recv_hwm(1000)
}

/// A connected inproc pair: `sender` connects to `receiver`.
async fn pair(sender: SocketType, receiver: SocketType, name: &str) -> (Socket, Socket) {
    let rx = Socket::new(receiver, options());
    let endpoint = rx.bind(test_support::inproc(name)).await.unwrap();
    let tx = Socket::new(sender, options());
    tx.connect(endpoint).await.unwrap();
    tx.wait_connected(1, DEADLINE).await.unwrap();
    rx.wait_connected(1, DEADLINE).await.unwrap();
    (tx, rx)
}

async fn queue(tx: &Socket) {
    for _ in 0..OPERATIONS {
        tx.send(message()).await.unwrap();
    }
}

/// Whether a task spawned before `OPERATIONS` ready steps ran among them.
async fn sibling_ran_during(mut step: impl AsyncFnMut()) -> bool {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    tokio::spawn(async move { flag.store(true, Ordering::Release) });

    tokio::time::timeout(DEADLINE, async {
        for _ in 0..OPERATIONS {
            step().await;
            if ran.load(Ordering::Acquire) {
                return true;
            }
        }

        false
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn a_receive_loop_lets_other_tasks_run_while_messages_wait() {
    let pairs = [
        (SocketType::Push, SocketType::Pull),
        (SocketType::Dealer, SocketType::Router),
        (SocketType::Scatter, SocketType::Gather),
        (SocketType::Pair, SocketType::Pair),
    ];

    let mut starved = Vec::new();
    for (sender, receiver) in pairs {
        let name = format!("recv-budget-recv-{receiver:?}");
        let (tx, rx) = pair(sender, receiver, &name).await;
        queue(&tx).await;

        if !sibling_ran_during(async || drop(rx.recv().await.unwrap())).await {
            starved.push(receiver);
        }
    }

    assert!(
        starved.is_empty(),
        "another task never ran during {OPERATIONS} ready receives on {starved:?}"
    );
}

#[tokio::test]
async fn a_receipt_receive_loop_lets_other_tasks_run_while_messages_wait() {
    let (tx, rx) = pair(SocketType::Push, SocketType::Pull, "recv-budget-receipt").await;
    queue(&tx).await;

    let ran = sibling_ran_during(async || {
        drop(rx.recv_with_receipt(None).await.unwrap());
    })
    .await;

    assert!(
        ran,
        "another task never ran during {OPERATIONS} ready receipt receives"
    );
}

#[tokio::test]
async fn a_batch_receive_loop_lets_other_tasks_run_while_messages_wait() {
    let (tx, rx) = pair(SocketType::Push, SocketType::Pull, "recv-budget-batch").await;
    queue(&tx).await;

    // Each batch call spends one unit, like a Tokio channel's.
    let ran = sibling_ran_during(async || {
        let mut out = Vec::new();
        assert_eq!(rx.recv_many_into(1, &mut out).await.unwrap(), 1);
    })
    .await;

    assert!(
        ran,
        "another task never ran during {OPERATIONS} ready batch receives"
    );
}

#[tokio::test]
async fn a_message_stream_lets_other_tasks_run_while_messages_wait() {
    let (tx, rx) = pair(SocketType::Push, SocketType::Pull, "recv-budget-stream").await;
    queue(&tx).await;

    let messages = rx.message();
    futures::pin_mut!(messages);
    let ran = sibling_ran_during(async || {
        drop(messages.next().await.unwrap().unwrap());
    })
    .await;

    assert!(
        ran,
        "another task never ran during {OPERATIONS} ready stream items"
    );
}
