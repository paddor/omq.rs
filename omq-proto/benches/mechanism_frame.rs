//! Per-frame CURVE overhead microbench.

use std::hint::black_box;
use std::time::Instant;

fn main() {
    let cipher = salsabox::BoxKey::from_shared_secret(&[0x42; 32]);
    let nonce = [0x11; 24];
    for size in [128, 2_048, 8_192] {
        let mut payload = vec![0xAC; size];
        let start = Instant::now();
        for _ in 0..1000 {
            black_box(cipher.encrypt_detached(&nonce, black_box(&mut payload[..])));
        }
        let ns = start.elapsed().as_nanos() / 1000;
        println!("{size:>6} bytes: {ns:>8} ns/op");
    }
}
