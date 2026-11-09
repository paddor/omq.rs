use super::*;
use windows::Win32::System::Threading::{OpenEventW, SYNCHRONIZATION_SYNCHRONIZE};
use windows::core::HSTRING;

#[test]
fn dropped_event_closes_its_handle() {
    let name = HSTRING::from(format!("omq-notify-test-{}", std::process::id()));
    // SAFETY: `name` outlives the call; no security attributes are passed.
    let handle = unsafe { CreateEventW(None, true, false, &name) }.unwrap();
    drop(Event(handle));

    // SAFETY: as above. A named event disappears with its last handle.
    let reopened = unsafe { OpenEventW(SYNCHRONIZATION_SYNCHRONIZE, false, &name) };
    assert!(reopened.is_err());
}

#[test]
fn notify_handle_signals_and_drains_recv_event() {
    let notify = WindowsNotifyHandle::new().unwrap();
    let recv = notify.recv_notifier();
    assert!(!recv.wait_for_readable(0));

    notify.signal_recv();
    assert!(recv.wait_for_readable(0));

    recv.drain();
    assert!(!recv.wait_for_readable(0));
}
