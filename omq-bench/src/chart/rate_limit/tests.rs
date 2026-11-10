use super::*;

fn campaign(id: &str) -> Vec<Row> {
    SIZES
        .iter()
        .flat_map(|&size| {
            [false, true].into_iter().flat_map(move |limited| {
                (1..=3).map(move |repeat| Row {
                    run_id: id.into(),
                    binary_sha256: "a".repeat(64),
                    transport: "tcp".into(),
                    processes: 2,
                    io_threads: 1,
                    cpu_affinity: "sender=1-2,receiver=3-4".into(),
                    netem: link(),
                    egress_bytes_per_second: limited.then_some(RATE),
                    burst_bytes: limited.then_some(BURST),
                    msg_size: size,
                    repeat,
                    rounds: 3,
                    warmup_seconds: 1.0,
                    duration_seconds: 10.0,
                    elapsed: 10.0,
                    count: f64::from(repeat * 100),
                    sender_cpu: f64::from(repeat),
                    receiver_cpu: 1.0,
                    payload_verified: true,
                })
            })
        })
        .collect()
}

#[test]
fn retains_complete_campaign_and_cpu_from_median_sample() {
    let mut rows = campaign("complete");
    let mut partial = campaign("partial");
    partial.pop();
    rows.extend(partial);
    let selected = select(rows);
    assert_eq!(selected.len(), SIZES.len() * 2);
    assert!(
        selected
            .iter()
            .all(|row| row.run_id == "complete" && row.repeat == 2)
    );

    for row in selected {
        assert!((row.sender_cpu - 2.0).abs() < f64::EPSILON);
    }
}

#[test]
fn rejects_mixed_binaries_and_mismatched_network_settings() {
    let mut rows = campaign("mixed");
    rows[0].binary_sha256 = "b".repeat(64);
    assert!(select(rows).is_empty());

    let mut rows = campaign("wrong-burst");
    rows.iter_mut()
        .find(|row| row.burst_bytes.is_some())
        .unwrap()
        .burst_bytes = Some(BURST * 2);
    assert!(select(rows).is_empty());

    let mut rows = campaign("wrong-link");
    rows[0].netem.segmentation_offloads = true;
    assert!(select(rows).is_empty());
}
