use super::*;

#[test]
fn every_pair_preserves_its_reply_route() {
    let ctx = omq_tokio::Context::new();
    for pair in [
        SocketPair::ReqRep,
        SocketPair::RouterDealer,
        SocketPair::RouterRouter,
        SocketPair::Pair,
        SocketPair::ClientServer,
        SocketPair::Peer,
        SocketPair::Channel,
    ] {
        let responder =
            ctx.blocking_socket(pair.responder(), pair.options(Options::default(), false));
        let endpoint = responder
            .bind(format!("inproc://latency-pair-{pair:?}"))
            .unwrap();
        let requester =
            ctx.blocking_socket(pair.requester(), pair.options(Options::default(), true));
        requester.connect(endpoint).unwrap();
        requester
            .wait_connected(1, std::time::Duration::from_secs(2))
            .unwrap();

        for size in [16, 64, 4096] {
            requester.send(pair.request(size)).unwrap();
            let request = responder
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            pair.validate(&request, size, false);
            responder.send(request).unwrap();
            let reply = requester
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            pair.validate(&reply, size, true);
        }
    }
}
