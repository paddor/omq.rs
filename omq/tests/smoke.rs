//! Smoke: the facade re-exports compile and at least one backend
//! constant resolves. Real socket-type / transport coverage lives in
//! the backend crates' own test suites.

use omq::{Endpoint, Options, SocketType};

#[test]
fn types_and_constants_re_exported() {
    assert!(matches!(SocketType::Push, SocketType::Push));
    let _ep = Endpoint::Inproc {
        name: "facade-smoke".into(),
    };
    let _opts = Options::default();
    assert!(omq::BACKEND == "compio" || omq::BACKEND == "tokio");
}
