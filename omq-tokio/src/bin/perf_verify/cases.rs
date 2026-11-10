//! The list of gate cases and the workload each one measures.

use super::contract::{Pattern, Shape};
use super::draft;
use super::transport::Transport;

#[derive(Debug)]
pub(super) enum Workload {
    Draft(draft::Workload),
    Reqrep,
    Pipeline {
        size: usize,
        io_threads: usize,
        transport: Transport,
    },
    Pubsub {
        size: usize,
        io_threads: usize,
        peers: usize,
        transport: Transport,
    },
    PubsubProcesses,
    Contract {
        pattern: Pattern,
        shape: Shape,
        size: usize,
    },
}

#[derive(Debug)]
pub(super) struct Case {
    pub(super) name: String,
    pub(super) workload: Workload,
}

pub(super) fn cases() -> Vec<Case> {
    let mut cases = vec![Case {
        name: "reqrep_ct.p50_256b_us".to_owned(),
        workload: Workload::Reqrep,
    }];
    let transports = [
        Transport::Tcp,
        #[cfg(feature = "quic")]
        Transport::Quic,
        #[cfg(feature = "dart")]
        Transport::Dart,
    ];

    for transport in transports {
        for io_threads in [1, 2] {
            add_pipeline_cases(&mut cases, transport, io_threads);
            // DART has no PUB/SUB gate.
            #[cfg(feature = "dart")]
            if matches!(transport, Transport::Dart) {
                continue;
            }

            add_pubsub_cases(&mut cases, transport, io_threads);
        }
    }

    cases.push(Case {
        name: "pubsub_2io.256b_32p_msgs_s".to_owned(),
        workload: Workload::PubsubProcesses,
    });

    cases.push(Case {
        name: "inproc_pushpull_1io.16b_msgs_s".to_owned(),
        workload: Workload::Pipeline {
            size: 16,
            io_threads: 1,
            transport: Transport::Inproc,
        },
    });

    contract_cases(&mut cases);
    draft::cases(&mut cases);
    cases
}

/// PUSH/PULL (SCATTER/GATHER on DART) throughput. Sizes above 1 KiB are
/// gated on TCP only.
fn add_pipeline_cases(cases: &mut Vec<Case>, transport: Transport, io_threads: usize) {
    let pipeline = match transport {
        #[cfg(feature = "dart")]
        Transport::Dart => "scattergather",
        _ => "pushpull",
    };

    for (size, suffix) in [(16, "16b"), (256, "256b"), (1024, "1k"), (16 * 1024, "16k")] {
        if size > 1024 && !matches!(transport, Transport::Tcp) {
            continue;
        }

        cases.push(Case {
            name: format!(
                "{}{pipeline}_{io_threads}io.{suffix}_msgs_s",
                transport.prefix()
            ),
            workload: Workload::Pipeline {
                size,
                io_threads,
                transport,
            },
        });
    }
}

/// PUB/SUB fan-out to four subscribers. Sizes above 1 KiB are gated on TCP
/// only.
fn add_pubsub_cases(cases: &mut Vec<Case>, transport: Transport, io_threads: usize) {
    for (size, suffix) in [(16, "16b"), (256, "256b"), (1024, "1k"), (4096, "4k")] {
        if size > 1024 && !matches!(transport, Transport::Tcp) {
            continue;
        }

        cases.push(Case {
            name: format!(
                "{}pubsub_{io_threads}io.{suffix}_msgs_s",
                transport.prefix()
            ),
            workload: Workload::Pubsub {
                size,
                io_threads,
                peers: 4,
                transport,
            },
        });
    }
}

/// Round trips across socket types and peer counts, where a latency
/// profile that copies or queues too much shows up as a loss to the
/// throughput profile, and the reverse for one-way streams.
fn contract_cases(cases: &mut Vec<Case>) {
    // At 4 MiB a payload copy on the caller costs more than the profile gains.
    let sizes = [(256, "256b"), (4 * 1024 * 1024, "4m")];
    let round_trips = [
        (Pattern::ReqRep, 1),
        (Pattern::ReqRep, 8),
        (Pattern::DealerRouter, 1),
        (Pattern::DealerRouter, 8),
        (Pattern::Peer, 1),
        (Pattern::Peer, 8),
        (Pattern::Pair, 1),
    ];

    for (pattern, peers) in round_trips {
        for (size, suffix) in sizes {
            cases.push(Case {
                name: format!("contract_rr.{}_{peers}p_{suffix}", pattern.label()),
                workload: Workload::Contract {
                    pattern,
                    shape: Shape::RoundTrip { peers },
                    size,
                },
            });
        }
    }

    for pattern in [Pattern::PushPull, Pattern::DealerRouter, Pattern::Pair] {
        for (size, suffix) in [(64, "64b"), (16 * 1024, "16k")] {
            cases.push(Case {
                name: format!("contract_stream.{}_{suffix}", pattern.label()),
                workload: Workload::Contract {
                    pattern,
                    shape: Shape::Stream,
                    size,
                },
            });
        }
    }
}
