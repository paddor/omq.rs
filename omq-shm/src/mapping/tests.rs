#![forbid(unsafe_code)]
use super::*;

#[test]
fn import_requires_all_size_seals_and_a_complete_header() {
    for seals in [
        SealFlags::empty(),
        SealFlags::GROW,
        SealFlags::SHRINK,
        SealFlags::SHRINK | SealFlags::GROW,
    ] {
        let file =
            File::from(rustix::fs::memfd_create("omq-invalid", MemfdFlags::ALLOW_SEALING).unwrap());
        file.set_len((HEADER + 16) as u64).unwrap();
        rustix::fs::fcntl_add_seals(&file, seals).unwrap();
        assert_eq!(
            Mapping::import(file).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    for length in [0, HEADER - 1, HEADER, HEADER + 16] {
        let file =
            File::from(rustix::fs::memfd_create("omq-invalid", MemfdFlags::ALLOW_SEALING).unwrap());
        file.set_len(length as u64).unwrap();
        rustix::fs::fcntl_add_seals(&file, SEALS).unwrap();
        assert_eq!(
            Mapping::import(file).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}

#[test]
fn imported_data_is_read_only_and_lease_cells_remain_writable() {
    for kind in [DATA, LEASES] {
        let source = Mapping::create(HEADER + 16, kind).unwrap();
        let imported = Mapping::import(source.descriptor().unwrap()).unwrap();

        assert_eq!(source.token, imported.token);
        assert_eq!(source.id, imported.id);
        assert_eq!(source.kind, imported.kind);
        assert_eq!(source.len(), imported.len());
        assert!(imported.file.is_none());
        assert!(imported.descriptor().is_err());

        if kind == DATA {
            assert!(!imported.writable);
            assert!(std::panic::catch_unwind(|| imported.atomic64(HEADER)).is_err());
        } else {
            imported
                .atomic64(HEADER)
                .store(7, std::sync::atomic::Ordering::Release);
            assert_eq!(
                source
                    .atomic64(HEADER)
                    .load(std::sync::atomic::Ordering::Acquire),
                7
            );
        }
    }
}

#[test]
fn receivers_cannot_write_a_data_region_through_any_descriptor_or_mapping() {
    use std::os::unix::fs::FileExt;
    use std::sync::atomic::Ordering;

    let source = Mapping::create(HEADER + 4096, DATA).unwrap();
    let file = source.descriptor().unwrap();
    let imported = Mapping::import(source.descriptor().unwrap()).unwrap();
    let offset = HEADER as u64;
    assert!(file.write_at(b"x", offset).is_err());

    assert!(
        rustix::fs::fallocate(
            &file,
            rustix::fs::FallocateFlags::PUNCH_HOLE | rustix::fs::FallocateFlags::KEEP_SIZE,
            offset,
            1,
        )
        .is_err()
    );

    assert!(Mapping::try_map_writable(&file, source.len()).is_err());
    assert!(imported.try_mprotect_writable().is_err());

    let reopened = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .unwrap();
    assert!(reopened.write_at(b"x", offset).is_err());
    assert!(Mapping::try_map_writable(&reopened, source.len()).is_err());

    let memory = std::fs::OpenOptions::new()
        .write(true)
        .open("/proc/self/mem")
        .unwrap();
    assert!(
        memory
            .write_at(b"x", (imported.address() + HEADER) as u64)
            .is_err()
    );

    // The creator's existing mapping still writes slots in place.
    source.atomic64(HEADER).store(7, Ordering::Release);
    assert_eq!(read_u64(imported.bytes(HEADER, 8), 0), 7);
}

#[test]
fn import_rejects_a_data_region_without_a_write_seal() {
    use std::os::unix::fs::FileExt;

    let length = HEADER + 16;
    let file =
        File::from(rustix::fs::memfd_create("omq-unsealed", MemfdFlags::ALLOW_SEALING).unwrap());
    file.set_len(length as u64).unwrap();
    let mut header = [0; HEADER];
    header[..REGION_MAGIC.len()].copy_from_slice(REGION_MAGIC);

    for (offset, value) in [(KIND_OFFSET, DATA), (LENGTH_OFFSET, length as u64)] {
        header[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    file.write_all_at(&header, 0).unwrap();
    rustix::fs::fcntl_add_seals(&file, SEALS).unwrap();
    let error = Mapping::import(file).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "SHM data region is not write-sealed");
}

#[test]
fn invalid_region_kind_and_header_length_are_rejected() {
    let source = Mapping::create(HEADER + 16, u64::MAX).unwrap();
    assert!(Mapping::import(source.descriptor().unwrap()).is_err());

    let source = Mapping::create(HEADER + 16, DATA).unwrap();
    source
        .atomic64(LENGTH_OFFSET)
        .store(u64::MAX, std::sync::atomic::Ordering::Release);
    assert!(Mapping::import(source.descriptor().unwrap()).is_err());

    source
        .atomic64(LENGTH_OFFSET)
        .store(source.len() as u64, std::sync::atomic::Ordering::Release);
    source
        .atomic64(ID_OFFSET)
        .store(u64::MAX, std::sync::atomic::Ordering::Release);
    assert!(Mapping::import(source.descriptor().unwrap()).is_err());
}

#[test]
fn sealed_region_cannot_shrink_grow_or_change_its_seals() {
    let source = Mapping::create(HEADER + 16, DATA).unwrap();
    let file = source.descriptor().unwrap();
    assert!(file.set_len(HEADER as u64).is_err());
    assert!(file.set_len((HEADER + 32) as u64).is_err());
    assert!(rustix::fs::fcntl_add_seals(&file, SealFlags::WRITE).is_err());
    assert_eq!(source.bytes(HEADER, 16), &[0; 16]);
}
