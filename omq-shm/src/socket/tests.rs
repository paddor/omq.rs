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
