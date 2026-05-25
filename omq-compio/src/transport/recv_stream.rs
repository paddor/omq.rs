use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use omq_proto::error::Error;

use crate::socket::DirectIoState;
use crate::transport::peer_io::SharedPeerIo;

pub(super) enum StreamArmOutcome {
    ClaimFlipped,
    Fed,
    Eof,
    StreamEnded,
    ProtoErr(Error),
    Err(std::io::Error),
    AccData(Bytes),
}

impl From<crate::socket::OneShotLargeRecvOutcome> for StreamArmOutcome {
    fn from(o: crate::socket::OneShotLargeRecvOutcome) -> Self {
        match o {
            crate::socket::OneShotLargeRecvOutcome::Skipped
            | crate::socket::OneShotLargeRecvOutcome::RearmMultiShot
            | crate::socket::OneShotLargeRecvOutcome::Took
            | crate::socket::OneShotLargeRecvOutcome::AccumulatePayload => Self::Fed,
            crate::socket::OneShotLargeRecvOutcome::IoErr(e) => Self::Err(e),
            crate::socket::OneShotLargeRecvOutcome::ProtoErr(e) => Self::ProtoErr(e),
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(super) async fn pull_stream(
    state: &Arc<DirectIoState>,
    peer_io: &SharedPeerIo,
    recv_active: bool,
    accumulating: bool,
) -> StreamArmOutcome {
    if recv_active {
        state.recv_state_changed.listen().await;
        return StreamArmOutcome::ClaimFlipped;
    }
    if accumulating {
        let mut sguard = state.recv_stream.0.lock().await;
        if state.recv_claim.load(Ordering::Acquire) == 1 {
            drop(sguard);
            state.recv_state_changed.listen().await;
            return StreamArmOutcome::ClaimFlipped;
        }
        return match sguard.as_mut() {
            Some(crate::socket::RecvStreamState::OneShot) => {
                drop(sguard);
                let payload_len = state.large_recv_pending.load(Ordering::Acquire);
                let fd = {
                    let io = peer_io.lock().expect("peer_io");
                    io.reader.fd_clone()
                };
                let mut restore = crate::socket::AccRestore {
                    state,
                    buf: state.pending_acc.lock().expect("pending_acc").take(),
                };
                let acc = restore.buf.as_mut().expect("pending_acc");
                if let Err(e) = fd.read_until(acc, payload_len).await {
                    return StreamArmOutcome::Err(e);
                }
                state.last_input_nanos.store(
                    state.hb_epoch.elapsed().as_nanos() as u64,
                    Ordering::Relaxed,
                );
                let payload = restore.buf.take().unwrap().freeze();
                state.large_recv_pending.store(0, Ordering::Release);
                let mut io = peer_io.lock().expect("peer_io");
                match io.codec.supply_payload(payload) {
                    Ok(()) => StreamArmOutcome::Fed,
                    Err(e) => StreamArmOutcome::ProtoErr(e),
                }
            }
            Some(crate::socket::RecvStreamState::MultiShot(cs)) => {
                let buf = compio::runtime::FutureExt::with_cancel(
                    futures::StreamExt::next(&mut cs.stream),
                    cs.cancel.clone(),
                )
                .await;
                match buf {
                    None => StreamArmOutcome::StreamEnded,
                    Some(Err(e)) => StreamArmOutcome::Err(e),
                    Some(Ok(buf)) if buf.is_empty() => StreamArmOutcome::Eof,
                    Some(Ok(buf)) => {
                        state.last_input_nanos.store(
                            state.hb_epoch.elapsed().as_nanos() as u64,
                            Ordering::Relaxed,
                        );
                        let bytes = bytes::Bytes::copy_from_slice(&buf[..]);
                        drop(buf);
                        StreamArmOutcome::AccData(bytes)
                    }
                }
            }
            None => StreamArmOutcome::Eof,
        };
    }
    let mut sguard = state.recv_stream.0.lock().await;
    if state.recv_claim.load(Ordering::Acquire) == 1 {
        drop(sguard);
        state.recv_state_changed.listen().await;
        return StreamArmOutcome::ClaimFlipped;
    }
    match sguard.as_mut() {
        None => StreamArmOutcome::Eof,
        Some(crate::socket::RecvStreamState::OneShot) => {
            crate::socket::one_shot_recv_and_feed(state, &mut sguard)
                .await
                .into()
        }
        Some(crate::socket::RecvStreamState::MultiShot(cs)) => {
            let buf = compio::runtime::FutureExt::with_cancel(
                futures::StreamExt::next(&mut cs.stream),
                cs.cancel.clone(),
            )
            .await;
            match buf {
                None => StreamArmOutcome::StreamEnded,
                Some(Err(e)) => StreamArmOutcome::Err(e),
                Some(Ok(buf)) => {
                    if buf.is_empty() {
                        return StreamArmOutcome::Eof;
                    }
                    state.last_input_nanos.store(
                        state.hb_epoch.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    let handle_result = {
                        let mut io = peer_io.lock().expect("peer_io");
                        let bytes = bytes::Bytes::copy_from_slice(&buf[..]);
                        drop(buf);
                        io.codec.handle_input(bytes)
                    };
                    match handle_result {
                        Err(e) => StreamArmOutcome::ProtoErr(e),
                        Ok(()) => crate::socket::try_one_shot_large_recv(state, &mut sguard)
                            .await
                            .into(),
                    }
                }
            }
        }
    }
}
