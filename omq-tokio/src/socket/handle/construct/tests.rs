use super::*;

#[test]
fn try_new_returns_invalid_options() {
    assert!(matches!(
        Socket::try_new(SocketType::Pair, Options::default().identity(vec![0])),
        Err(Error::Config(_))
    ));
    assert!(matches!(
        Socket::try_new(SocketType::Pair, Options::default().conflate(true)),
        Err(Error::Config(_))
    ));
}
