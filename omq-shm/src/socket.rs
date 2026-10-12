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
mod tests {
    use super::*;
    use rustix::io::FdFlags;
    use std::io::Read;
    use std::os::unix::net::UnixStream;

    #[test]
    fn received_descriptors_are_cloexec_and_keep_the_region_alive() {
        let (first, second) = UnixStream::pair().unwrap();
        let region =
            crate::mapping::Mapping::create(crate::mapping::HEADER + 128, crate::mapping::DATA)
                .unwrap();
        let files = VecDeque::from([region.descriptor().unwrap()]);
        assert_eq!(send(&first, &[IoSlice::new(b"x")], &files).unwrap(), 1);
        drop(files);
        drop(region);
        let (count, mut files) = receive(&second, &mut [0]).unwrap();
        assert_eq!(count, 1);
        assert_eq!(files.len(), 1);
        let file = files.pop().unwrap();
        assert!(
            rustix::io::fcntl_getfd(&file)
                .unwrap()
                .contains(FdFlags::CLOEXEC)
        );
        assert!(crate::mapping::Mapping::import(file).is_ok());
    }

    #[test]
    fn unexpected_ancillary_records_still_close_all_received_descriptors() {
        let (first, second) = UnixStream::pair().unwrap();
        rustix::net::sockopt::set_socket_passcred(&second, true).unwrap();
        let (reader, writer) = rustix::pipe::pipe_with(
            rustix::pipe::PipeFlags::CLOEXEC | rustix::pipe::PipeFlags::NONBLOCK,
        )
        .unwrap();
        let mut reader = File::from(reader);
        let files = VecDeque::from([File::from(writer)]);
        assert_eq!(send(&first, &[IoSlice::new(b"x")], &files).unwrap(), 1);
        drop(files);
        assert_eq!(
            receive(&second, &mut [0]).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            reader.read(&mut [0]).unwrap(),
            0,
            "a received writer FD leaked"
        );
    }
}
