use super::*;
use bytes::Bytes;

#[test]
fn frame_flags_consts() {
    assert_eq!(
        FrameFlags::LAST,
        FrameFlags {
            more: false,
            command: false
        }
    );
    assert_eq!(
        FrameFlags::MORE,
        FrameFlags {
            more: true,
            command: false
        }
    );
    assert_eq!(
        FrameFlags::COMMAND,
        FrameFlags {
            more: false,
            command: true
        }
    );
}

#[test]
fn frame_constructors() {
    let f = Frame::data(Bytes::from_static(b"x"), false);
    assert_eq!(f.flags, FrameFlags::LAST);
    assert_eq!(f.payload.len(), 1);

    let f = Frame::data(Bytes::from_static(b"x"), true);
    assert_eq!(f.flags, FrameFlags::MORE);

    let f = Frame::command(Bytes::from_static(b"READY"));
    assert_eq!(f.flags, FrameFlags::COMMAND);
}
