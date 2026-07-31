use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::options::WssTls;
use omq_tokio::{
    Endpoint, MechanismPeerInfo, Message, MonitorEvent, MonitorStream, Options, Socket, SocketType,
};

fn accept_alice(peer: &MechanismPeerInfo) -> bool {
    peer.username.as_deref() == Some("alice") && peer.password.as_deref() == Some("secret")
}

fn options(auth: &str) -> Options {
    match auth {
        "plain" => Options::default().plain_server(accept_alice),
        "null" => Options::default(),
        other => panic!("unknown auth mode: {other}"),
    }
}

fn options_with_identity(auth: &str, identity: impl Into<Bytes>) -> Options {
    options(auth).identity(identity)
}

async fn wait_handshake(sock: &Socket) {
    let mut monitor = sock.monitor();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match monitor.recv().await {
                Ok(MonitorEvent::HandshakeSucceeded { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before handshake: {e:?}"),
            }
        }
    })
    .await
    .expect("handshake timed out");
}

async fn wait_join(mut monitor: MonitorStream) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match monitor.recv().await {
                Ok(MonitorEvent::JoinReceived { .. }) => return,
                Ok(_) => {}
                Err(e) => panic!("monitor closed before JOIN: {e:?}"),
            }
        }
    })
    .await
    .expect("JOIN timed out");
}

fn print_endpoint(endpoint: &Endpoint) {
    println!("ENDPOINT {endpoint}");
    io::stdout().flush().unwrap();
}

type BrowserState = Arc<Mutex<HashMap<String, String>>>;

struct BrowserDelayedBind {
    endpoint: Endpoint,
    options: Options,
    delay_ms: u64,
}

type BrowserDelayedBinds = Arc<Mutex<HashMap<String, BrowserDelayedBind>>>;

fn record_browser(state: &BrowserState, key: &str, value: impl Into<String>) {
    state
        .lock()
        .expect("browser state mutex poisoned")
        .insert(key.to_string(), value.into());
}

fn read_browser(state: &BrowserState, key: &str) -> String {
    state
        .lock()
        .expect("browser state mutex poisoned")
        .get(key)
        .cloned()
        .unwrap_or_default()
}

fn clear_browser(state: &BrowserState) {
    state.lock().expect("browser state mutex poisoned").clear();
}

fn browser_ws(host: &str, base: u16, offset: u16) -> Endpoint {
    format!("ws://{host}:{}/", base + offset).parse().unwrap()
}

fn browser_lz4_ws(host: &str, base: u16, offset: u16) -> Endpoint {
    format!("lz4+ws://{host}:{}/", base + offset)
        .parse()
        .unwrap()
}

fn browser_wss(host: &str, base: u16, offset: u16) -> Endpoint {
    format!("wss://{host}:{}/", base + offset).parse().unwrap()
}

fn self_signed_tls() -> (Vec<u8>, Vec<u8>) {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let cert_pem = certified.cert.pem().into_bytes();
    let key_pem = certified.signing_key.serialize_pem().into_bytes();
    (cert_pem, key_pem)
}

fn wss_options(cert_pem: &[u8], key_pem: &[u8]) -> Options {
    Options {
        wss_tls: WssTls {
            server_cert_pem: Some(cert_pem.to_vec()),
            server_key_pem: Some(key_pem.to_vec()),
            accept_invalid_certs: false,
        },
        ..Options::default()
    }
}

fn plain_wss_options(cert_pem: &[u8], key_pem: &[u8]) -> Options {
    Options {
        wss_tls: WssTls {
            server_cert_pem: Some(cert_pem.to_vec()),
            server_key_pem: Some(key_pem.to_vec()),
            accept_invalid_certs: false,
        },
        ..Options::default().plain_server(accept_alice)
    }
}

fn part_string(msg: &Message, idx: usize) -> String {
    String::from_utf8_lossy(&msg.part_bytes(idx).unwrap_or_default()).into_owned()
}

async fn browser_control_rep(rep: Socket, state: BrowserState, delayed_binds: BrowserDelayedBinds) {
    loop {
        let msg = rep.recv().await.expect("browser control recv");
        let cmd = part_string(&msg, 0);
        let reply = if cmd == "ping" {
            "pong".to_string()
        } else if cmd == "clear" {
            clear_browser(&state);
            "ok".to_string()
        } else if let Some(key) = cmd.strip_prefix("get:") {
            read_browser(&state, key)
        } else if let Some(key) = cmd.strip_prefix("bind-delayed:") {
            let delayed = delayed_binds
                .lock()
                .expect("browser delayed binds mutex poisoned")
                .remove(key);
            if let Some(delayed) = delayed {
                tokio::spawn(browser_pull_delayed_bind(
                    delayed.endpoint,
                    key.to_string(),
                    delayed.options,
                    delayed.delay_ms,
                    state.clone(),
                ));
                "ok".to_string()
            } else {
                format!("missing:{key}")
            }
        } else {
            format!("unknown:{cmd}")
        };
        rep.send(Message::single(reply))
            .await
            .expect("browser control send");
    }
}

async fn browser_rep_echo(rep: Socket, name: &'static str, state: BrowserState) {
    loop {
        let msg = rep.recv().await.expect("browser rep recv");
        let req = part_string(&msg, 0);
        record_browser(&state, name, &req);
        rep.send(Message::single(format!("reply:{req}")))
            .await
            .expect("browser rep send");
    }
}

async fn browser_pull_record(pull: Socket, name: &'static str, state: BrowserState) {
    loop {
        let msg = pull.recv().await.expect("browser pull recv");
        record_browser(&state, name, part_string(&msg, 0));
    }
}

async fn browser_push_on_handshake(
    push: Socket,
    mut monitor: MonitorStream,
    name: &'static str,
    payload: &'static str,
    state: BrowserState,
) {
    loop {
        match monitor.recv().await {
            Ok(MonitorEvent::HandshakeSucceeded { .. }) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                push.send(Message::single(payload))
                    .await
                    .expect("browser push send");
                record_browser(&state, name, payload);
            }
            Ok(_) => {}
            Err(e) => panic!("browser push monitor closed: {e:?}"),
        }
    }
}

async fn browser_pub_repeat(
    pub_: Socket,
    name: &'static str,
    topic: &'static str,
    payload: &'static str,
    state: BrowserState,
) {
    loop {
        pub_.send(Message::multipart([topic, payload]))
            .await
            .expect("browser pub send");
        record_browser(&state, name, format!("{topic}|{payload}"));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn browser_sub_record(sub: Socket, name: &'static str, state: BrowserState) {
    loop {
        let msg = sub.recv().await.expect("browser sub recv");
        record_browser(
            &state,
            name,
            format!("{}|{}", part_string(&msg, 0), part_string(&msg, 1)),
        );
    }
}

async fn browser_bind_retry(endpoint: &Endpoint, options: Options, label: &str) -> Socket {
    for _ in 0..80 {
        let pull = Socket::new(SocketType::Pull, options.clone());
        if pull.bind(endpoint.clone()).await.is_ok() {
            return pull;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{label} bind timed out");
}

async fn browser_pull_restart_loop(
    endpoint: Endpoint,
    mut pull: Socket,
    options: Options,
    state: BrowserState,
) {
    loop {
        let msg = pull.recv().await.expect("browser restart first recv");
        record_browser(&state, "restart_first", part_string(&msg, 0));
        pull.close().await.expect("browser restart first close");

        tokio::time::sleep(Duration::from_millis(300)).await;

        let pull2 = browser_bind_retry(&endpoint, options.clone(), "browser restart second").await;
        let msg = pull2.recv().await.expect("browser restart second recv");
        record_browser(&state, "restart_second", part_string(&msg, 0));
        pull2.close().await.expect("browser restart second close");

        tokio::time::sleep(Duration::from_millis(300)).await;
        pull = browser_bind_retry(&endpoint, options.clone(), "browser restart first").await;
    }
}

async fn browser_pull_delayed_bind(
    endpoint: Endpoint,
    name: String,
    options: Options,
    delay_ms: u64,
    state: BrowserState,
) {
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    let pull = bind_socket(SocketType::Pull, endpoint, options).await;
    loop {
        let msg = pull.recv().await.expect("browser delayed pull recv");
        record_browser(&state, &name, part_string(&msg, 0));
    }
}

async fn bind_socket(socket_type: SocketType, endpoint: Endpoint, options: Options) -> Socket {
    let socket = Socket::new(socket_type, options);
    socket.bind(endpoint).await.expect("browser bind");
    socket
}

async fn browser_bind(base: u16, host: String) {
    let state: BrowserState = Arc::new(Mutex::new(HashMap::new()));
    let delayed_binds: BrowserDelayedBinds = Arc::new(Mutex::new(HashMap::new()));
    let plain = Options::default().plain_server(accept_alice);
    let (cert_pem, key_pem) = self_signed_tls();

    let control = bind_socket(
        SocketType::Rep,
        browser_ws(&host, base, 0),
        Options::default(),
    )
    .await;
    let rep_ws = bind_socket(
        SocketType::Rep,
        browser_ws(&host, base, 1),
        Options::default(),
    )
    .await;
    let pull_ws = bind_socket(
        SocketType::Pull,
        browser_ws(&host, base, 2),
        Options::default(),
    )
    .await;
    let push_ws = bind_socket(
        SocketType::Push,
        browser_ws(&host, base, 3),
        Options::default(),
    )
    .await;
    let push_ws_monitor = push_ws.monitor();

    let sub_ws = Socket::new(SocketType::Sub, Options::default());
    sub_ws
        .subscribe("news.")
        .await
        .expect("browser sub subscribe");
    sub_ws
        .bind(browser_ws(&host, base, 4))
        .await
        .expect("browser sub bind");

    let pub_ws = bind_socket(
        SocketType::Pub,
        browser_ws(&host, base, 5),
        Options::default(),
    )
    .await;
    let pull_plain = bind_socket(SocketType::Pull, browser_ws(&host, base, 6), plain.clone()).await;
    let pull_plain_reject = bind_socket(SocketType::Pull, browser_ws(&host, base, 7), plain).await;
    let pull_lz4 = bind_socket(
        SocketType::Pull,
        browser_lz4_ws(&host, base, 8),
        Options::default(),
    )
    .await;
    let push_lz4 = bind_socket(
        SocketType::Push,
        browser_lz4_ws(&host, base, 9),
        Options::default(),
    )
    .await;
    let push_lz4_monitor = push_lz4.monitor();
    let restart_endpoint = browser_ws(&host, base, 10);
    let restart_pull = bind_socket(
        SocketType::Pull,
        restart_endpoint.clone(),
        Options::default(),
    )
    .await;
    let wss_rep = bind_socket(
        SocketType::Rep,
        browser_wss(&host, base, 11),
        wss_options(&cert_pem, &key_pem),
    )
    .await;
    let wss_pull = bind_socket(
        SocketType::Pull,
        browser_wss(&host, base, 12),
        wss_options(&cert_pem, &key_pem),
    )
    .await;
    let wss_push = bind_socket(
        SocketType::Push,
        browser_wss(&host, base, 13),
        wss_options(&cert_pem, &key_pem),
    )
    .await;
    let wss_push_monitor = wss_push.monitor();
    let wss_pull_plain = bind_socket(
        SocketType::Pull,
        browser_wss(&host, base, 14),
        plain_wss_options(&cert_pem, &key_pem),
    )
    .await;
    let delayed_ws_endpoint = browser_ws(&host, base, 18);
    let delayed_lz4_ws_endpoint = browser_lz4_ws(&host, base, 19);
    let delayed_wss_endpoint = browser_wss(&host, base, 20);

    for (name, endpoint, options) in [
        (
            "connect_before_bind_ws",
            delayed_ws_endpoint,
            Options::default(),
        ),
        (
            "connect_before_bind_lz4_ws",
            delayed_lz4_ws_endpoint,
            Options::default(),
        ),
        (
            "connect_before_bind_wss",
            delayed_wss_endpoint,
            wss_options(&cert_pem, &key_pem),
        ),
    ] {
        delayed_binds
            .lock()
            .expect("browser delayed binds mutex poisoned")
            .insert(
                name.to_string(),
                BrowserDelayedBind {
                    endpoint,
                    options,
                    delay_ms: 750,
                },
            );
    }

    tokio::spawn(browser_control_rep(
        control,
        state.clone(),
        delayed_binds.clone(),
    ));
    tokio::spawn(browser_rep_echo(rep_ws, "rep_ws", state.clone()));
    tokio::spawn(browser_pull_record(pull_ws, "pull_ws", state.clone()));
    tokio::spawn(browser_push_on_handshake(
        push_ws,
        push_ws_monitor,
        "push_ws",
        "push-ws-from-rust",
        state.clone(),
    ));
    tokio::spawn(browser_sub_record(sub_ws, "sub_ws", state.clone()));
    tokio::spawn(browser_pub_repeat(
        pub_ws,
        "pub_ws",
        "news.rust",
        "pub-ws-from-rust",
        state.clone(),
    ));
    tokio::spawn(browser_pull_record(pull_plain, "pull_plain", state.clone()));
    tokio::spawn(browser_pull_record(
        pull_plain_reject,
        "pull_plain_reject",
        state.clone(),
    ));
    tokio::spawn(browser_pull_record(pull_lz4, "pull_lz4", state.clone()));
    tokio::spawn(browser_push_on_handshake(
        push_lz4,
        push_lz4_monitor,
        "push_lz4",
        "push-lz4-from-rust",
        state.clone(),
    ));
    tokio::spawn(browser_pull_restart_loop(
        restart_endpoint,
        restart_pull,
        Options::default(),
        state.clone(),
    ));
    tokio::spawn(browser_rep_echo(wss_rep, "wss_rep", state.clone()));
    tokio::spawn(browser_pull_record(wss_pull, "wss_pull", state.clone()));
    tokio::spawn(browser_push_on_handshake(
        wss_push,
        wss_push_monitor,
        "wss_push",
        "push-wss-from-rust",
        state.clone(),
    ));
    tokio::spawn(browser_pull_record(
        wss_pull_plain,
        "wss_pull_plain",
        state.clone(),
    ));
    println!("BROWSER_READY");
    println!("control ws://{host}:{base}/");
    for (name, offset, scheme, note) in [
        ("rep_ws", 1, "ws", ""),
        ("pull_ws", 2, "ws", ""),
        ("push_ws", 3, "ws", ""),
        ("sub_ws", 4, "ws", ""),
        ("pub_ws", 5, "ws", ""),
        ("pull_plain", 6, "ws", " PLAIN"),
        ("pull_plain_reject", 7, "ws", " PLAIN"),
        ("pull_lz4", 8, "lz4+ws", ""),
        ("push_lz4", 9, "lz4+ws", ""),
        ("restart_pull", 10, "ws", ""),
        ("wss_rep", 11, "wss", ""),
        ("wss_pull", 12, "wss", ""),
        ("wss_push", 13, "wss", ""),
        ("wss_pull_plain", 14, "wss", " PLAIN"),
        ("connect_before_bind_ws", 18, "ws", ""),
        ("connect_before_bind_lz4_ws", 19, "lz4+ws", ""),
        ("connect_before_bind_wss", 20, "wss", ""),
    ] {
        println!("{name} {scheme}://{host}:{}/{}", base + offset, note);
    }
    io::stdout().flush().unwrap();

    std::future::pending::<()>().await;
}

async fn pull_bind(endpoint: Endpoint, expected: String, auth: &str) {
    let pull = Socket::new(SocketType::Pull, options(auth));
    let bound = pull.bind(endpoint).await.expect("pull bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .expect("pull recv timed out")
        .expect("pull recv");
    assert_eq!(msg.part_bytes(0).unwrap(), expected.as_bytes());
}

async fn pull_restart_bind(endpoint: Endpoint, before: String, after: String, auth: &str) {
    let pull1 = Socket::new(SocketType::Pull, options(auth));
    let bound = pull1.bind(endpoint).await.expect("pull1 bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), pull1.recv())
        .await
        .expect("pull1 recv timed out")
        .expect("pull1 recv");
    assert_eq!(msg.part_bytes(0).unwrap(), before.as_bytes());

    pull1.close().await.expect("pull1 close");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let pull2 = Socket::new(SocketType::Pull, options(auth));
    let mut bound_again = false;
    for _ in 0..40 {
        if pull2.bind(bound.clone()).await.is_ok() {
            bound_again = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(bound_again, "pull2 failed to bind after pull1 closed");

    let msg = tokio::time::timeout(Duration::from_secs(10), pull2.recv())
        .await
        .expect("pull2 recv timed out")
        .expect("pull2 recv");
    assert_eq!(msg.part_bytes(0).unwrap(), after.as_bytes());
}

async fn pull_delayed_bind(endpoint: Endpoint, expected: String, delay_ms: u64, auth: &str) {
    print_endpoint(&endpoint);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let pull = Socket::new(SocketType::Pull, options(auth));
    pull.bind(endpoint).await.expect("delayed pull bind");

    let msg = tokio::time::timeout(Duration::from_secs(10), pull.recv())
        .await
        .expect("delayed pull recv timed out")
        .expect("delayed pull recv");
    assert_eq!(msg.part_bytes(0).unwrap(), expected.as_bytes());
}

async fn push_bind(endpoint: Endpoint, payload: String, auth: &str) {
    let push = Socket::new(SocketType::Push, options(auth));
    let bound = push.bind(endpoint).await.expect("push bind");
    print_endpoint(&bound);

    wait_handshake(&push).await;
    push.send(Message::from(Bytes::from(payload)))
        .await
        .expect("push send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn rep_bind(endpoint: Endpoint, expected: String, reply: String, auth: &str) {
    let rep = Socket::new(SocketType::Rep, options(auth));
    let bound = rep.bind(endpoint).await.expect("rep bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), rep.recv())
        .await
        .expect("rep recv timed out")
        .expect("rep recv");
    assert_eq!(msg.part_bytes(0).unwrap(), expected.as_bytes());
    rep.send(Message::from(Bytes::from(reply)))
        .await
        .expect("rep send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn pub_bind(endpoint: Endpoint, topic: String, payload: String, auth: &str) {
    let pub_ = Socket::new(SocketType::Pub, options(auth));
    let bound = pub_.bind(endpoint).await.expect("pub bind");
    print_endpoint(&bound);

    wait_handshake(&pub_).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    pub_.send(Message::multipart([topic, payload]))
        .await
        .expect("pub send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn sub_bind(endpoint: Endpoint, topic: String, payload: String, auth: &str) {
    let sub = Socket::new(SocketType::Sub, options(auth));
    sub.subscribe("news.").await.expect("sub subscribe");
    let bound = sub.bind(endpoint).await.expect("sub bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), sub.recv())
        .await
        .expect("sub recv timed out")
        .expect("sub recv");
    assert_eq!(msg.part_bytes(0).unwrap(), topic.as_bytes());
    assert_eq!(msg.part_bytes(1).unwrap(), payload.as_bytes());
}

async fn gather_bind(endpoint: Endpoint, expected: String, auth: &str) {
    let gather = Socket::new(SocketType::Gather, options(auth));
    let bound = gather.bind(endpoint).await.expect("gather bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), gather.recv())
        .await
        .expect("gather recv timed out")
        .expect("gather recv");
    assert_eq!(msg, Message::single(expected));
}

async fn scatter_bind(endpoint: Endpoint, payload: String, auth: &str) {
    let scatter = Socket::new(SocketType::Scatter, options(auth));
    let bound = scatter.bind(endpoint).await.expect("scatter bind");
    print_endpoint(&bound);

    wait_handshake(&scatter).await;
    scatter
        .send(Message::single(payload))
        .await
        .expect("scatter send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn server_bind(endpoint: Endpoint, expected: String, reply: String, auth: &str) {
    let server = Socket::new(SocketType::Server, options(auth));
    let bound = server.bind(endpoint).await.expect("server bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), server.recv())
        .await
        .expect("server recv timed out")
        .expect("server recv");
    assert_eq!(msg.part_bytes(1).unwrap(), expected.as_bytes());
    let routing_id = msg.part_bytes(0).unwrap();
    server
        .send(Message::multipart([routing_id, Bytes::from(reply)]))
        .await
        .expect("server send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn radio_bind(endpoint: Endpoint, group: String, payload: String, auth: &str) {
    let radio = Socket::new(SocketType::Radio, options(auth));
    let monitor = radio.monitor();
    let bound = radio.bind(endpoint).await.expect("radio bind");
    print_endpoint(&bound);

    wait_join(monitor).await;
    radio
        .send(Message::multipart([group, payload]))
        .await
        .expect("radio send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn dish_bind(endpoint: Endpoint, group: String, payload: String, auth: &str) {
    let dish = Socket::new(SocketType::Dish, options(auth));
    dish.join(group.clone()).await.expect("dish join");
    let bound = dish.bind(endpoint).await.expect("dish bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), dish.recv())
        .await
        .expect("dish recv timed out")
        .expect("dish recv");
    assert_eq!(msg.part_bytes(0).unwrap(), group.as_bytes());
    assert_eq!(msg.part_bytes(1).unwrap(), payload.as_bytes());
}

async fn channel_bind(endpoint: Endpoint, expected: String, reply: String, auth: &str) {
    let channel = Socket::new(SocketType::Channel, options(auth));
    let bound = channel.bind(endpoint).await.expect("channel bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), channel.recv())
        .await
        .expect("channel recv timed out")
        .expect("channel recv");
    assert_eq!(msg, Message::single(expected));
    channel
        .send(Message::single(reply))
        .await
        .expect("channel send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn router_bind(
    endpoint: Endpoint,
    identity: String,
    request_a: String,
    request_b: String,
    reply_a: String,
    reply_b: String,
    auth: &str,
) {
    let router = Socket::new(SocketType::Router, options(auth));
    let bound = router.bind(endpoint).await.expect("router bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), router.recv())
        .await
        .expect("router recv timed out")
        .expect("router recv");
    assert_eq!(msg.part_bytes(0).unwrap(), identity.as_bytes());
    assert_eq!(msg.part_bytes(1).unwrap(), request_a.as_bytes());
    assert_eq!(msg.part_bytes(2).unwrap(), request_b.as_bytes());

    router
        .send(Message::multipart([
            Bytes::from(identity),
            Bytes::from(reply_a),
            Bytes::from(reply_b),
        ]))
        .await
        .expect("router send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn dealer_bind(
    endpoint: Endpoint,
    identity: String,
    request_a: String,
    request_b: String,
    reply_a: String,
    reply_b: String,
    auth: &str,
) {
    let dealer = Socket::new(
        SocketType::Dealer,
        options_with_identity(auth, Bytes::from(identity)),
    );
    let bound = dealer.bind(endpoint).await.expect("dealer bind");
    print_endpoint(&bound);

    wait_handshake(&dealer).await;
    dealer
        .send(Message::multipart([request_a, request_b]))
        .await
        .expect("dealer send");

    let msg = tokio::time::timeout(Duration::from_secs(10), dealer.recv())
        .await
        .expect("dealer recv timed out")
        .expect("dealer recv");
    assert_eq!(msg.part_bytes(0).unwrap(), reply_a.as_bytes());
    assert_eq!(msg.part_bytes(1).unwrap(), reply_b.as_bytes());
}

async fn pair_bind(
    endpoint: Endpoint,
    request_a: String,
    request_b: String,
    reply_a: String,
    reply_b: String,
    auth: &str,
) {
    let pair = Socket::new(SocketType::Pair, options(auth));
    let bound = pair.bind(endpoint).await.expect("pair bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), pair.recv())
        .await
        .expect("pair recv timed out")
        .expect("pair recv");
    assert_eq!(msg.part_bytes(0).unwrap(), request_a.as_bytes());
    assert_eq!(msg.part_bytes(1).unwrap(), request_b.as_bytes());

    pair.send(Message::multipart([reply_a, reply_b]))
        .await
        .expect("pair send");
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn xpub_bind(endpoint: Endpoint, topic: String, payload: String, auth: &str) {
    let xpub = Socket::new(SocketType::XPub, options(auth));
    let bound = xpub.bind(endpoint).await.expect("xpub bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), xpub.recv())
        .await
        .expect("xpub recv timed out")
        .expect("xpub recv");
    let sub = msg.part_bytes(0).unwrap();
    assert_eq!(sub.first(), Some(&1));
    assert_eq!(&sub[1..], topic.as_bytes());

    for _ in 0..10 {
        xpub.send(Message::single(payload.clone()))
            .await
            .expect("xpub send");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn xsub_bind(endpoint: Endpoint, _topic: String, payload: String, auth: &str) {
    let xsub = Socket::new(SocketType::XSub, options(auth));
    xsub.subscribe("news.").await.expect("xsub subscribe");
    let bound = xsub.bind(endpoint).await.expect("xsub bind");
    print_endpoint(&bound);

    let msg = tokio::time::timeout(Duration::from_secs(10), xsub.recv())
        .await
        .expect("xsub recv timed out")
        .expect("xsub recv");
    assert_eq!(msg.part_bytes(0).unwrap(), payload.as_bytes());
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("mode");

    match mode.as_str() {
        "browser-bind" => {
            let base = args
                .next()
                .unwrap_or_else(|| "9105".to_string())
                .parse()
                .expect("base port");
            let host = args.next().unwrap_or_else(|| "127.0.0.1".to_string());
            browser_bind(base, host).await;
        }
        "pull-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pull_bind(endpoint, payload, &auth).await;
        }
        "push-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            push_bind(endpoint, payload, &auth).await;
        }
        "pull-restart-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let before = args.next().expect("before");
            let after = args.next().expect("after");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pull_restart_bind(endpoint, before, after, &auth).await;
        }
        "pull-delayed-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let delay_ms = args
                .next()
                .unwrap_or_else(|| "750".to_string())
                .parse()
                .expect("delay ms");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pull_delayed_bind(endpoint, payload, delay_ms, &auth).await;
        }
        "rep-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let reply = args.next().expect("reply");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            rep_bind(endpoint, payload, reply, &auth).await;
        }
        "pub-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let topic = args.next().expect("topic");
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pub_bind(endpoint, topic, payload, &auth).await;
        }
        "sub-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let topic = args.next().expect("topic");
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            sub_bind(endpoint, topic, payload, &auth).await;
        }
        "gather-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            gather_bind(endpoint, payload, &auth).await;
        }
        "scatter-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            scatter_bind(endpoint, payload, &auth).await;
        }
        "server-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let reply = args.next().expect("reply");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            server_bind(endpoint, payload, reply, &auth).await;
        }
        "radio-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let group = args.next().expect("group");
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            radio_bind(endpoint, group, payload, &auth).await;
        }
        "dish-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let group = args.next().expect("group");
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            dish_bind(endpoint, group, payload, &auth).await;
        }
        "channel-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let payload = args.next().expect("payload");
            let reply = args.next().expect("reply");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            channel_bind(endpoint, payload, reply, &auth).await;
        }
        "router-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let identity = args.next().expect("identity");
            let request_a = args.next().expect("request a");
            let request_b = args.next().expect("request b");
            let reply_a = args.next().expect("reply a");
            let reply_b = args.next().expect("reply b");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            router_bind(
                endpoint, identity, request_a, request_b, reply_a, reply_b, &auth,
            )
            .await;
        }
        "dealer-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let identity = args.next().expect("identity");
            let request_a = args.next().expect("request a");
            let request_b = args.next().expect("request b");
            let reply_a = args.next().expect("reply a");
            let reply_b = args.next().expect("reply b");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            dealer_bind(
                endpoint, identity, request_a, request_b, reply_a, reply_b, &auth,
            )
            .await;
        }
        "pair-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let request_a = args.next().expect("request a");
            let request_b = args.next().expect("request b");
            let reply_a = args.next().expect("reply a");
            let reply_b = args.next().expect("reply b");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            pair_bind(endpoint, request_a, request_b, reply_a, reply_b, &auth).await;
        }
        "xpub-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let topic = args.next().expect("topic");
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            xpub_bind(endpoint, topic, payload, &auth).await;
        }
        "xsub-bind" => {
            let endpoint: Endpoint = args.next().expect("endpoint").parse().unwrap();
            let topic = args.next().expect("topic");
            let payload = args.next().expect("payload");
            let auth = args.next().unwrap_or_else(|| "null".to_string());
            xsub_bind(endpoint, topic, payload, &auth).await;
        }
        other => panic!("unknown mode: {other}"),
    }
}
