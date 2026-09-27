//! Socket-pair setup shared by the blocking and async latency peers.

use bytes::Bytes;
use omq_tokio::options::WorkloadProfile;
use omq_tokio::{Message, Options, SocketType};

const REQUESTER: &[u8] = b"bench-requester";
const RESPONDER: &[u8] = b"bench-responder";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SocketPair {
    ReqRep,
    RouterDealer,
    RouterRouter,
    Pair,
    ClientServer,
    Peer,
    Channel,
}

impl SocketPair {
    pub(crate) fn from_env() -> Self {
        match std::env::var("OMQ_BENCH_LATENCY_PAIR")
            .as_deref()
            .unwrap_or("req-rep")
        {
            "req-rep" => Self::ReqRep,
            "router-dealer" => Self::RouterDealer,
            "router-router" => Self::RouterRouter,
            "pair" => Self::Pair,
            "client-server" => Self::ClientServer,
            "peer" => Self::Peer,
            "channel" => Self::Channel,
            other => panic!("unknown OMQ_BENCH_LATENCY_PAIR: {other}"),
        }
    }

    pub(crate) fn requester(self) -> SocketType {
        match self {
            Self::ReqRep => SocketType::Req,
            Self::RouterDealer => SocketType::Dealer,
            Self::RouterRouter => SocketType::Router,
            Self::Pair => SocketType::Pair,
            Self::ClientServer => SocketType::Client,
            Self::Peer => SocketType::Peer,
            Self::Channel => SocketType::Channel,
        }
    }

    pub(crate) fn responder(self) -> SocketType {
        match self {
            Self::ReqRep => SocketType::Rep,
            Self::RouterDealer | Self::RouterRouter => SocketType::Router,
            Self::Pair => SocketType::Pair,
            Self::ClientServer => SocketType::Server,
            Self::Peer => SocketType::Peer,
            Self::Channel => SocketType::Channel,
        }
    }

    pub(crate) fn options(self, options: Options, requester: bool) -> Options {
        let options = match std::env::var("OMQ_BENCH_WORKLOAD_PROFILE")
            .as_deref()
            .unwrap_or("default")
        {
            "default" => options,
            "latency" => options.workload_profile(WorkloadProfile::Latency),
            "throughput" => options.workload_profile(WorkloadProfile::Throughput),
            other => panic!("unknown OMQ_BENCH_WORKLOAD_PROFILE: {other}"),
        };
        let kind = if requester {
            self.requester()
        } else {
            self.responder()
        };
        options
            .identity(Bytes::from_static(if requester {
                REQUESTER
            } else {
                RESPONDER
            }))
            .router_mandatory(matches!(kind, SocketType::Router | SocketType::Peer))
    }

    pub(crate) fn request(self, size: usize) -> Message {
        let payload = Bytes::from(vec![b'x'; size]);
        if matches!(self.requester(), SocketType::Router | SocketType::Peer) {
            Message::multipart([Bytes::from_static(RESPONDER), payload])
        } else {
            Message::single(payload)
        }
    }

    /// Echoing the received message preserves ROUTER/PEER's identity frame
    /// and SERVER's `routing_id`. No synthetic envelope is included in size.
    pub(crate) fn validate(self, message: &Message, size: usize, reply: bool) {
        let kind = if reply {
            self.requester()
        } else {
            self.responder()
        };
        let routed = matches!(kind, SocketType::Router | SocketType::Peer);
        assert_eq!(
            message.len(),
            if routed { 2 } else { 1 },
            "wrong frame count for {self:?}"
        );
        if routed {
            assert_eq!(
                message.get(0).unwrap(),
                if reply { RESPONDER } else { REQUESTER }
            );
        }
        if kind == SocketType::Server {
            assert!(
                message.routing_id().is_some(),
                "SERVER request has no routing id"
            );
        }
        let body = message.get(usize::from(routed)).unwrap();
        assert_eq!(body.len(), size, "wrong body length for {self:?}");
        assert!(
            body.iter().all(|&byte| byte == b'x'),
            "corrupt body for {self:?}"
        );
    }
}

#[cfg(test)]
mod tests {
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
                .bind(format!("inproc://latency-pair-{pair:?}").parse().unwrap())
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
}
