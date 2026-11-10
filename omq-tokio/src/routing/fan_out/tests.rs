use super::{FanOutMode, FanOutMutePolicy, FanOutSend, fan_out_mute_policy, yield_interval};
use omq_proto::options::{OnMute, Options};
use omq_proto::proto::SocketType;

#[test]
fn yield_interval_scales_with_message_size() {
    assert_eq!(yield_interval(1, 16), 512);
    assert_eq!(yield_interval(1, 256), 512);
    assert_eq!(yield_interval(1, 1024), 256);
    assert_eq!(yield_interval(1, 4096), 64);
    assert_eq!(yield_interval(1, 16 * 1024), 16);
}

#[test]
fn yield_interval_yields_every_send_without_active_targets() {
    assert_eq!(yield_interval(0, 16), 1);
    assert_eq!(yield_interval(0, 16 * 1024), 1);
}

#[test]
fn drop_oldest_policy_is_pub_xpub_radio_only() {
    let options = Options::default().on_mute(OnMute::DropOldest);

    assert_eq!(
        fan_out_mute_policy(SocketType::Pub, &options),
        FanOutMutePolicy::DropOldest
    );
    assert_eq!(
        fan_out_mute_policy(SocketType::Radio, &options),
        FanOutMutePolicy::DropOldest
    );
    assert_eq!(
        fan_out_mute_policy(SocketType::XPub, &options),
        FanOutMutePolicy::DropOldest
    );

    let mut nodrop_options = options;
    nodrop_options.xpub_nodrop = true;
    assert_eq!(
        fan_out_mute_policy(SocketType::XPub, &nodrop_options),
        FanOutMutePolicy::Block
    );
}

#[tokio::test]
async fn fan_out_send_new_wires_drop_oldest_policy() {
    let options = Options::default().on_mute(OnMute::DropOldest);
    let io_pool = crate::context::IoPoolHandle::none();

    for (socket_type, mode) in [
        (SocketType::Pub, FanOutMode::SubscriptionPrefix),
        (SocketType::XPub, FanOutMode::SubscriptionPrefix),
        (SocketType::Radio, FanOutMode::Group),
    ] {
        let send = FanOutSend::new(socket_type, &options, mode, &io_pool);
        assert_eq!(send.config.mute_policy, FanOutMutePolicy::DropOldest);
        send.shutdown();
    }
}
