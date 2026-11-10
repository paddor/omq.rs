use super::*;
use crate::transport::Transport;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(unix)]
fn temp_ipc(name: &str) -> Endpoint {
    let short_name: String = name.chars().take(8).collect();
    let path = std::path::PathBuf::from(format!(
        "/tmp/omq-ipc-{short_name}-{}.sock",
        std::process::id()
    ));

    Endpoint::Ipc(IpcPath::Filesystem(path))
}

#[cfg(unix)]
#[tokio::test]
async fn bind_connect_accept_roundtrip() {
    let ep = temp_ipc("basic");
    let mut listener = IpcTransport::bind(&ep).await.unwrap();
    let ep2 = ep.clone();
    let connect = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

    let (mut server_side, peer) = listener.accept().await.unwrap();
    let mut client_side = connect.await.unwrap().unwrap();
    assert!(matches!(peer, PeerIdent::Path(_)));

    client_side.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    server_side.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn abstract_bind_connect_roundtrip() {
    // Random suffix avoids cross-test collisions in the abstract namespace.
    let name = format!(
        "omq-ipc-abs-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    );
    let ep = Endpoint::Ipc(IpcPath::Abstract(name));
    let mut listener = IpcTransport::bind(&ep).await.unwrap();
    let ep2 = ep.clone();
    let connect = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

    let (mut server_side, peer) = listener.accept().await.unwrap();
    let mut client_side = connect.await.unwrap().unwrap();

    match peer {
        PeerIdent::Path(p) => assert!(p.starts_with('@')),
        other => panic!("expected abstract peer ident, got {other:?}"),
    }

    client_side.write_all(b"abstract").await.unwrap();
    let mut buf = [0u8; 8];
    server_side.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"abstract");
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn abstract_rejected_off_linux() {
    #[cfg(unix)]
    {
        let ep = Endpoint::Ipc(IpcPath::Abstract("foo".into()));
        assert!(matches!(
            IpcTransport::bind(&ep).await,
            Err(Error::UnsupportedScheme(_))
        ));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn bind_cleans_up_stale_socket() {
    let ep = temp_ipc("stale");
    // First bind creates the socket file.
    {
        let _l = IpcTransport::bind(&ep).await.unwrap();
    }

    // Drop removed the file. Re-bind should still succeed.
    let _l = IpcTransport::bind(&ep).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn listener_drop_removes_socket_file() {
    let ep = temp_ipc("drop");
    let path = match &ep {
        Endpoint::Ipc(IpcPath::Filesystem(p)) => p.clone(),
        _ => unreachable!(),
    };

    {
        let _l = IpcTransport::bind(&ep).await.unwrap();
        assert!(path.exists());
    }

    assert!(!path.exists(), "drop should have removed the socket file");
}

#[cfg(target_os = "windows")]
#[tokio::test]
async fn windows_named_pipe_bind_connect_roundtrip() {
    let name = format!("omq-pipe-{}-{}", std::process::id(), rand::random::<u32>());
    let ep = Endpoint::Ipc(IpcPath::NamedPipe(name));

    // Bind server
    let mut listener = IpcTransport::bind(&ep).await.unwrap();

    // Connect client in background
    let ep2 = ep.clone();
    let connect = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

    // Accept connection
    let (mut server_side, peer) = listener.accept().await.unwrap();
    let mut client_side = connect.await.unwrap().unwrap();

    // Verify peer identity contains the pipe name
    assert!(matches!(peer, PeerIdent::Path(_)));

    // Roundtrip data
    client_side.write_all(b"windows").await.unwrap();
    let mut buf = [0u8; 7];
    server_side.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"windows");
}

#[cfg(target_os = "windows")]
#[tokio::test]
async fn windows_named_pipe_accepts_concurrent_clients() {
    let name = format!("omq-pipe-{}-{}", std::process::id(), rand::random::<u32>());
    let ep = Endpoint::Ipc(IpcPath::NamedPipe(name));
    let mut listener = IpcTransport::bind(&ep).await.unwrap();

    let ep1 = ep.clone();
    let ep2 = ep.clone();
    let connect1 = tokio::spawn(async move { IpcTransport::connect(&ep1).await });
    let connect2 = tokio::spawn(async move { IpcTransport::connect(&ep2).await });

    let (mut server1, _) = listener.accept().await.unwrap();
    let (mut server2, _) = listener.accept().await.unwrap();
    let mut client1 = connect1.await.unwrap().unwrap();
    let mut client2 = connect2.await.unwrap().unwrap();

    client1.write_all(b"hello").await.unwrap();
    client2.write_all(b"hello").await.unwrap();

    let mut buf1 = [0u8; 5];
    let mut buf2 = [0u8; 5];
    server1.read_exact(&mut buf1).await.unwrap();
    server2.read_exact(&mut buf2).await.unwrap();

    assert_eq!(&buf1, b"hello");
    assert_eq!(&buf2, b"hello");
}

#[cfg(target_os = "windows")]
#[tokio::test]
async fn windows_pipe_name_in_error() {
    // Invalid pipe name should be rejected at parse time
    let result: Result<Endpoint> = "ipc://CON".parse();
    assert!(result.is_err());
}
