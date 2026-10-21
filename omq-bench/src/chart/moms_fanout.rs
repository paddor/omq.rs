//! Verified delivery to every subscriber socket, normalized per subscriber.

use std::collections::BTreeMap;

use plotters::style::RGBColor;
use serde_json::Value;

use super::common::{self, FairnessEntry, FairnessMap, Impl, ValMap};
use crate::jsonl;

const SIZES: &[u64] = common::COMPARISON_SIZES;
const READERS: &[u64] = &[1, 4, 32];
const fn omq(key: &'static str, label: &'static str, color: RGBColor) -> Impl {
    Impl {
        key,
        label,
        color,
        threads: "1 IO; no recv spin",
    }
}

const NETWORK: &[Impl] = &[
    omq("omq-tcp", "OMQ / TCP", common::C_OMQ_1T),
    omq("omq-quic", "OMQ / QUIC", common::C_OMQ_QUIC),
    omq("omq-dart", "OMQ / DART", RGBColor(255, 183, 77)),
    Impl {
        key: "zenoh-tcp",
        label: "Zenoh / TCP",
        color: common::C_ZENOH,
        threads: "1 session/sub; blocking",
    },
    Impl {
        key: "rabbitmq-tcp",
        label: "RabbitMQ / AMQP",
        color: common::C_RABBITMQ,
        threads: "blocking receive",
    },
];
const LOCAL: &[Impl] = &[
    omq("omq-shm", "OMQ / SHM", RGBColor(29, 78, 216)),
    omq("omq-shm-pool", "OMQ / SHM pool", RGBColor(0, 191, 165)),
    omq("omq-ipc", "OMQ / IPC", RGBColor(239, 83, 168)),
    Impl {
        key: "zenoh-unix",
        label: "Zenoh / Unix",
        color: common::C_ZENOH,
        threads: "1 session/sub; blocking",
    },
];

pub(super) struct Chart {
    filename: &'static str,
    title: &'static str,
    implementations: &'static [Impl],
    rows: Vec<(u64, ValMap, ValMap, FairnessMap)>,
}

fn external_receive_policy(row: &Value, implementation: &str) -> bool {
    if implementation.starts_with("zenoh-") {
        row["recv_mode"] == "blocking" && row["receiver_context"] == "per-subscriber-session"
    } else if implementation == "rabbitmq-tcp" {
        row["recv_mode"] == "blocking"
    } else {
        true
    }
}

fn verified(row: &Value, implementation: &str, size: u64, readers: u64) -> bool {
    let base = implementation
        .strip_suffix("-spin50")
        .unwrap_or(implementation);
    omq_receive_policy(row, base, base != implementation)
        && external_receive_policy(row, base)
        && shm_storage(row, base)
        && cohort_layout(row, base, size, readers)
        && rounds_verified(row, readers)
}

/// OMQ cohorts use blocking receive, HWM 1000, no IO spin, and the receive
/// spin of their series.
fn omq_receive_policy(row: &Value, implementation: &str, spin50: bool) -> bool {
    !implementation.starts_with("omq-")
        || (row["recv_mode"] == "blocking"
            && row["recv_spin_us"] == if spin50 { 50 } else { 0 }
            && row["io_spin_us"] == 0
            && row["queue_capacity"] == 1000)
}

/// OMQ SHM cohorts publish ordinary or pooled messages under the default
/// SHM memory limit.
fn shm_storage(row: &Value, implementation: &str) -> bool {
    if !implementation.starts_with("omq-shm") {
        return true;
    }
    let pool = implementation == "omq-shm-pool";
    row["transport"] == "shm"
        && row["storage"] == if pool { "preallocated-shm" } else { "ordinary" }
        && row["pool_slots"] == if pool { 1024 } else { 0 }
        && row["shm_max_memory"] == omq_tokio::options::DEFAULT_SHM_MAX_MEMORY
}

/// Cohort identity, timing, CPU placement, and process layout. Aeron with
/// 32 readers runs one receiver process and Media Driver per reader.
fn cohort_layout(row: &Value, implementation: &str, size: u64, readers: u64) -> bool {
    let separate_aeron = implementation.starts_with("aeron-") && readers == 32;
    let layout = row["kind"] == "pub_sub_verified"
        && row["impl"] == implementation
        && row["msg_size"] == size
        && row["peers"] == readers
        && row["subscriber_processes"] == if separate_aeron { readers } else { 1 }
        && row["subscriber_threads"] == if separate_aeron { 1 } else { readers }
        && row["process_layout"]
            == if separate_aeron {
                "independent-receiver-processes"
            } else {
                "two-process-shared-receiver"
            }
        && row["warmup_seconds"] == 1
        && row["seconds"] == 3
        && row["publisher_cpus"] == "1,2"
        && row["subscriber_cpus"] == "3,4"
        && row["copy_in"] == true
        && row["binary_sha256"].as_str().is_some();
    layout
        && (!separate_aeron
            || (row["receiver_context"] == "per-process"
                && row["media_drivers"] == if implementation == "aeron-ipc" { 1 } else { 33 }
                && (implementation != "aeron-udp" || row["udp_delivery"] == "mdc-dynamic-min")))
}

/// Three rounds, each delivering every offered message to every reader.
fn rounds_verified(row: &Value, readers: u64) -> bool {
    let Some(rounds) = row["rounds"].as_array().filter(|rounds| rounds.len() == 3) else {
        return false;
    };
    rounds
        .iter()
        .enumerate()
        .all(|(index, round)| round_verified(index, round, readers))
}

/// One round: complete reader counts, positive reader intervals, at least
/// 3 s of publishing, and a rate over the slowest reader's drain.
fn round_verified(index: usize, round: &Value, readers: u64) -> bool {
    let offered = round["offered"].as_u64().unwrap_or(0);
    let Some(counts) = round["reader_counts"].as_array() else {
        return false;
    };
    let Some(times) = round["reader_elapsed"].as_array() else {
        return false;
    };
    let sender = round["sender_elapsed"].as_f64().unwrap_or(0.0);
    let elapsed = round["elapsed"].as_f64().unwrap_or(0.0);
    let rate = round["msgs_s"].as_f64().unwrap_or(0.0);
    let slowest = times
        .iter()
        .filter_map(Value::as_f64)
        .fold(sender, f64::max);

    round["round"] == index + 1
        && offered > 0
        && counts.len() as u64 == readers
        && counts.iter().all(|count| count.as_u64() == Some(offered))
        && times.len() as u64 == readers
        && times
            .iter()
            .all(|time| time.as_f64().is_some_and(|v| v.is_finite() && v > 0.0))
        && sender.is_finite()
        && sender >= 3.0
        && elapsed.is_finite()
        && (elapsed - slowest).abs() < 1e-6
        && rate.is_finite()
        && rate > 0.0
        && (rate - offered as f64 / elapsed).abs() < rate * 1e-6
        && ["missing", "duplicates", "gaps", "corrupt"]
            .iter()
            .all(|field| round[field] == 0)
}

pub(super) fn prepare() -> Vec<Chart> {
    let cached: Vec<_> = jsonl::load_jsonl::<Value>(&jsonl::cache_dir().join("mom-fanout.jsonl"))
        .into_iter()
        .map(|(_, row)| row)
        .collect();
    let mut charts = Vec::new();
    for (filename, title, implementations) in [
        (
            "fanout_network.svg",
            "Network pub/sub throughput per subscriber",
            NETWORK,
        ),
        (
            "fanout_local.svg",
            "Local pub/sub throughput per subscriber",
            LOCAL,
        ),
    ] {
        let rows = READERS
            .iter()
            .map(|&readers| reader_row(&cached, implementations, readers))
            .collect();
        charts.push(Chart {
            filename,
            title,
            implementations,
            rows,
        });
    }
    charts
}

/// One chart row: the median of three verified rounds per implementation
/// and size, with all three rounds as the spread.
fn reader_row(
    cached: &[Value],
    implementations: &[Impl],
    readers: u64,
) -> (u64, ValMap, ValMap, FairnessMap) {
    let mut bandwidth = ValMap::new();
    let mut messages = ValMap::new();
    let mut spread = FairnessMap::new();
    for implementation in implementations {
        for &size in SIZES {
            let row = cached
                .iter()
                .rev()
                .find(|row| verified(row, implementation.key, size, readers))
                .unwrap_or_else(|| {
                    panic!(
                        "missing verified fan-out cohort: {} {size} B {readers} readers; run scripts/rivals/run_fanout.rb",
                        implementation.key
                    )
                });
            let rates = sorted_round_rates(row);
            messages
                .entry(size)
                .or_default()
                .insert(implementation.key.into(), rates[1]);
            bandwidth
                .entry(size)
                .or_default()
                .insert(implementation.key.into(), rates[1] * size as f64 / 1e6);
            spread.entry(size).or_default().insert(
                implementation.key.into(),
                FairnessEntry {
                    min: rates[0],
                    p25: rates[0].midpoint(rates[1]),
                    median: rates[1],
                    p75: rates[1].midpoint(rates[2]),
                    max: rates[2],
                },
            );
        }
    }
    (readers, bandwidth, messages, spread)
}

/// Per-round message rates of a verified row, ascending.
fn sorted_round_rates(row: &Value) -> Vec<f64> {
    let mut rates: Vec<_> = row["rounds"]
        .as_array()
        .unwrap()
        .iter()
        .map(|round| round["msgs_s"].as_f64().unwrap())
        .collect();
    rates.sort_by(f64::total_cmp);
    rates
}

pub(super) fn draw(charts: &[Chart]) {
    for chart in charts {
        let rows: Vec<_> = chart
            .rows
            .iter()
            .map(
                |(readers, bandwidth, messages, spread)| common::ThroughputRow {
                    peers: *readers,
                    tput: bandwidth,
                    msgs: messages,
                    fairness: Some(spread),
                },
            )
            .collect();
        let path = common::out_dir().join("moms").join(chart.filename);
        let spec = common::ChartSpec {
            out_path: &path,
            title: chart.title,
            sizes: SIZES,
            impls: chart.implementations,
        };
        let no_cpu = BTreeMap::new();
        let columns = common::CpuColumns {
            cpu: &no_cpu,
            snd_label: "",
            rcv_label: "",
        };
        common::draw_multirow_throughput_log_gbs(
            &spec,
            &rows,
            &columns,
            &|readers| {
                let suffix = if readers == 1 { "" } else { "s" };
                format!("{readers} subscriber{suffix}")
            },
            common::detect_hardware().as_deref(),
        )
        .expect("draw verified MOM fan-out chart");
        eprintln!("Written: {}", path.display());
    }
}

pub(crate) fn generate() {
    let charts = prepare();
    std::fs::create_dir_all(common::out_dir().join("moms")).expect("create MOM chart directory");
    draw(&charts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn complete() -> Value {
        let rounds: Vec<_> = (1..=3)
            .map(|round| {
                json!({
                    "round":round, "offered":3000, "reader_counts":[3000,3000,3000,3000],
                    "reader_elapsed":[3.0,3.0,3.0,4.0], "sender_elapsed":3.0,
                    "elapsed":4.0,"msgs_s":750.0,"missing":0,"duplicates":0,"gaps":0,"corrupt":0
                })
            })
            .collect();
        json!({"kind":"pub_sub_verified","impl":"omq-tcp","msg_size":16,"peers":4,
            "recv_mode":"blocking","recv_spin_us":0,"io_spin_us":0,"queue_capacity":1000,
            "subscriber_processes":1,"subscriber_threads":4,"process_layout":"two-process-shared-receiver","warmup_seconds":1,"seconds":3,
            "publisher_cpus":"1,2","subscriber_cpus":"3,4","copy_in":true,
            "binary_sha256":"test","rounds":rounds})
    }

    #[test]
    fn omq_rows_require_fixed_hwm() {
        let mut row = complete();
        assert!(verified(&row, "omq-tcp", 16, 4));
        for capacity in [json!(256), Value::Null] {
            row["queue_capacity"] = capacity;
            assert!(!verified(&row, "omq-tcp", 16, 4));
        }
    }

    #[test]
    fn spin_variants_require_the_actual_blocking_receive_policy() {
        let mut row = complete();
        assert!(verified(&row, "omq-tcp", 16, 4));
        assert!(!verified(&row, "omq-tcp-spin50", 16, 4));
        row["recv_spin_us"] = json!(50);
        assert!(!verified(&row, "omq-tcp", 16, 4));
        assert!(verified(&row, "omq-tcp-spin50", 16, 4));
        row["recv_mode"] = json!("yield-poll");
        assert!(!verified(&row, "omq-tcp-spin50", 16, 4));
        row["recv_mode"] = Value::Null;
        row["recv_spin_us"] = Value::Null;
        row["io_spin_us"] = Value::Null;
        assert!(!verified(&row, "omq-tcp", 16, 4));
    }

    #[test]
    fn external_rows_require_blocking_receive() {
        for implementation in ["zenoh-tcp", "zenoh-unix", "rabbitmq-tcp"] {
            let mut row = complete();
            row["impl"] = json!(implementation);
            row["queue_capacity"] = json!(256);
            row["recv_spin_us"] = Value::Null;
            row["io_spin_us"] = Value::Null;
            if implementation.starts_with("zenoh-") {
                row["receiver_context"] = json!("per-subscriber-session");
            }
            assert!(verified(&row, implementation, 16, 4));
            row["recv_mode"] = json!("yield-poll");
            assert!(!verified(&row, implementation, 16, 4));
            if implementation.starts_with("zenoh-") {
                row["recv_mode"] = json!("blocking");
                row["receiver_context"] = json!("shared");
                assert!(!verified(&row, implementation, 16, 4));
            }
        }
    }

    #[test]
    fn shm_rows_require_the_recorded_storage_and_memory_limit() {
        for implementation in ["omq-shm", "omq-shm-pool"] {
            let mut row = complete();
            row["impl"] = json!(implementation);
            row["transport"] = json!("shm");
            row["storage"] = json!(if implementation == "omq-shm-pool" {
                "preallocated-shm"
            } else {
                "ordinary"
            });
            row["pool_slots"] = json!(if implementation == "omq-shm-pool" {
                1024
            } else {
                0
            });
            row["shm_max_memory"] = json!(omq_tokio::options::DEFAULT_SHM_MAX_MEMORY);
            assert!(verified(&row, implementation, 16, 4));
            row["shm_max_memory"] = Value::Null;
            assert!(!verified(&row, implementation, 16, 4));
            row["shm_max_memory"] = json!(omq_tokio::options::DEFAULT_SHM_MAX_MEMORY);
            row["storage"] = json!("unknown");
            assert!(!verified(&row, implementation, 16, 4));
        }
    }

    #[test]
    fn requires_every_reader_and_includes_slowest_drain() {
        let mut row = complete();
        assert!(verified(&row, "omq-tcp", 16, 4));
        row["rounds"][0]["msgs_s"] = json!(1000.0);
        assert!(!verified(&row, "omq-tcp", 16, 4));
        row = complete();
        row["rounds"][1]["reader_counts"][3] = json!(2999);
        assert!(!verified(&row, "omq-tcp", 16, 4));
        row = complete();
        row["rounds"][2]["gaps"] = json!(1);
        assert!(!verified(&row, "omq-tcp", 16, 4));
        row = complete();
        row["subscriber_processes"] = json!(4);
        assert!(!verified(&row, "omq-tcp", 16, 4));
    }

    #[test]
    fn aeron_32_requires_separate_receivers_and_drivers() {
        let mut row = complete();
        row["impl"] = json!("aeron-udp");
        row["peers"] = json!(32);
        row["subscriber_processes"] = json!(32);
        row["subscriber_threads"] = json!(1);
        row["process_layout"] = json!("independent-receiver-processes");
        row["receiver_context"] = json!("per-process");
        row["media_drivers"] = json!(33);
        row["udp_delivery"] = json!("mdc-dynamic-min");
        for round in row["rounds"].as_array_mut().unwrap() {
            round["reader_counts"] = json!(vec![3000; 32]);
            round["reader_elapsed"] = json!(vec![4.0; 32]);
        }
        assert!(verified(&row, "aeron-udp", 16, 32));
        row["subscriber_processes"] = json!(1);
        assert!(!verified(&row, "aeron-udp", 16, 32));
        row["subscriber_processes"] = json!(32);
        row["media_drivers"] = json!(2);
        assert!(!verified(&row, "aeron-udp", 16, 32));
    }
}
