use super::*;

#[test]
fn try_new_returns_invalid_options() {
    assert!(matches!(
        Socket::try_new(
            SocketType::Pair,
            Options::default().identity(omq_proto::generated_identity(7))
        ),
        Err(Error::Config(_))
    ));
    assert!(matches!(
        Socket::try_new(SocketType::Pair, Options::default().conflate(true)),
        Err(Error::Config(_))
    ));
}
