//! Nonblocking ancillary I/O with owned descriptor cleanup on every error path.
#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::os::fd::AsFd;

use rustix::net::{
    RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags, SendAncillaryBuffer,
    SendAncillaryMessage, SendFlags,
};

pub(crate) const SEND_LIMIT: usize = 64;
const RECEIVE_LIMIT: usize = 253;
const CONTROL_BYTES: usize = rustix::cmsg_space!(ScmRights(RECEIVE_LIMIT), ScmCredentials(1));

pub(crate) fn send(
    socket: impl AsFd,
    bytes: &[IoSlice<'_>],
    descriptors: &VecDeque<File>,
) -> io::Result<usize> {
    assert!(descriptors.len() <= SEND_LIMIT);
    let mut storage = [MaybeUninit::uninit(); CONTROL_BYTES];
    let mut control = SendAncillaryBuffer::new(&mut storage);
    let mut files = [socket.as_fd(); SEND_LIMIT];

    for (target, file) in files.iter_mut().zip(descriptors) {
        *target = file.as_fd();
    }

    if !descriptors.is_empty() {
        assert!(control.push(SendAncillaryMessage::ScmRights(&files[..descriptors.len()])));
    }

    Ok(rustix::net::sendmsg(
        &socket,
        bytes,
        &mut control,
        SendFlags::DONTWAIT | SendFlags::NOSIGNAL,
    )?)
}

pub(crate) fn receive(socket: impl AsFd, bytes: &mut [u8]) -> io::Result<(usize, Vec<File>)> {
    if bytes.is_empty() {
        return Ok((0, Vec::new()));
    }

    let mut storage = [MaybeUninit::uninit(); CONTROL_BYTES];
    let mut control = RecvAncillaryBuffer::new(&mut storage);
    let received = rustix::net::recvmsg(
        socket,
        &mut [IoSliceMut::new(bytes)],
        &mut control,
        RecvFlags::DONTWAIT | RecvFlags::CMSG_CLOEXEC,
    )?;
    let mut files = Vec::new();
    let mut invalid = received.flags.contains(ReturnFlags::CTRUNC);

    // Drain every record before rejecting: OwnedFd closes rights following an
    // unexpected record, including anything not consumed from the iterator.
    for record in control.drain() {
        if let RecvAncillaryMessage::ScmRights(rights) = record {
            files.extend(rights.map(File::from));
        } else {
            invalid = true;
        }
    }

    if invalid {
        return Err(crate::mapping::invalid(
            "invalid or truncated SHM ancillary message",
        ));
    }

    Ok((received.bytes, files))
}

#[cfg(all(test, not(miri)))]
mod tests;
