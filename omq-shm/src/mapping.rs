//! The Linux mapping boundary. All references are checked against a sealed
//! mapping; payload slots are disjoint and mutable only before publication.
//! Data regions are write-sealed: only the creator's mapping can modify them.

use std::fs::File;
use std::io;
#[cfg(not(miri))]
use std::os::fd::AsRawFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64};

#[cfg(not(miri))]
use rustix::fs::{MemfdFlags, SealFlags};
#[cfg(not(miri))]
use rustix::mm::{MapFlags, MprotectFlags, ProtFlags};

pub(crate) const HEADER: usize = 64;
pub(crate) const DATA: u64 = 1;
pub(crate) const LEASES: u64 = 2;
#[cfg(not(miri))]
const SEALS: SealFlags = SealFlags::SHRINK
    .union(SealFlags::GROW)
    .union(SealFlags::SEAL);
#[cfg(not(miri))]
const WRITE_SEALS: SealFlags = SealFlags::WRITE.union(SealFlags::FUTURE_WRITE);
const REGION_MAGIC: &[u8; 8] = b"OMQSHM02";
const TOKEN_OFFSET: usize = REGION_MAGIC.len();
const KIND_OFFSET: usize = TOKEN_OFFSET + std::mem::size_of::<u64>();
const LENGTH_OFFSET: usize = KIND_OFFSET + std::mem::size_of::<u64>();
// Bytes 32..48 hold the completion table's close flag and wake counters.
const ID_OFFSET: usize = 48;

#[derive(Debug)]
pub(crate) struct Mapping {
    #[cfg(not(miri))]
    file: File,
    #[cfg(miri)]
    allocation: std::alloc::Layout,
    address: NonNull<u8>,
    length: usize,
    writable: bool,
    pub(crate) token: u64,
    pub(crate) id: u32,
    pub(crate) kind: u64,
    pub(crate) reservation: Option<crate::memory::Reservation>,
}

// SAFETY: immutable metadata and a stable shared mapping. Payload mutation is
// confined to disjoint slots with exclusive pool checkout. Lease bytes are
// accessed only as aligned, lock-free atomics, shared across processes. Carrier
// bytes are owned by the SPSC writer until publication and reader until return.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Mapping {
    #[cfg(not(miri))]
    pub(crate) fn create(length: usize, kind: u64) -> io::Result<Self> {
        if !(HEADER..=isize::MAX as usize).contains(&length) {
            return Err(invalid("invalid SHM mapping size"));
        }
        let file = File::from(rustix::fs::memfd_create(
            "omq-shm",
            MemfdFlags::CLOEXEC | MemfdFlags::ALLOW_SEALING,
        )?);
        file.set_len(length as u64)?;
        rustix::fs::fcntl_add_seals(&file, SealFlags::SHRINK | SealFlags::GROW)?;
        let token = rand::random::<u64>();
        let mut mapping = Self::map(file, length, true, token)?;
        mapping.kind = kind;
        mapping.initialize_header();
        // FUTURE_WRITE keeps this writable mapping, but no descriptor can write,
        // punch, or newly map a data region writable. Receivers complete
        // leases in place, so lease tables stay writable.
        let seals = if kind == DATA {
            SealFlags::FUTURE_WRITE | SealFlags::SEAL
        } else {
            SealFlags::SEAL
        };
        rustix::fs::fcntl_add_seals(&mapping.file, seals)?;
        Ok(mapping)
    }

    // Miri cannot create shared memfds. Only allocation and teardown differ;
    // every pointer, slice, atomic, pool, and owner operation remains production code.
    #[cfg(miri)]
    pub(crate) fn create(length: usize, kind: u64) -> io::Result<Self> {
        if !(HEADER..=isize::MAX as usize).contains(&length) {
            return Err(invalid("invalid SHM mapping size"));
        }
        let allocation = std::alloc::Layout::from_size_align(length, HEADER)
            .map_err(|_| invalid("invalid SHM mapping layout"))?;
        // SAFETY: nonzero, valid layout; the raw allocation has no borrowed owner.
        let address = NonNull::new(unsafe { std::alloc::alloc_zeroed(allocation) })
            .ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let mut mapping = Self {
            allocation,
            address,
            length,
            writable: true,
            token: rand::random(),
            id: rand::random(),
            kind,
            reservation: None,
        };
        mapping.initialize_header();
        Ok(mapping)
    }

    fn initialize_header(&mut self) {
        let fields = [
            (TOKEN_OFFSET, self.token),
            (KIND_OFFSET, self.kind),
            (LENGTH_OFFSET, self.length as u64),
            (ID_OFFSET, u64::from(self.id)),
        ];
        // SAFETY: no reference has escaped this newly created region.
        let header = unsafe { self.bytes_mut(0, HEADER) };
        header[..REGION_MAGIC.len()].copy_from_slice(REGION_MAGIC);
        for (offset, value) in fields {
            header[offset..offset + std::mem::size_of::<u64>()]
                .copy_from_slice(&value.to_le_bytes());
        }
    }

    #[cfg(not(miri))]
    pub(crate) fn import(file: File) -> io::Result<Self> {
        let seals = rustix::fs::fcntl_get_seals(&file)
            .ok()
            .filter(|seals| seals.contains(SEALS))
            .ok_or_else(|| invalid("SHM region is not size-sealed"))?;
        let length = usize::try_from(file.metadata()?.len())
            .map_err(|_| invalid("SHM region exceeds address space"))?;
        if !(HEADER..=isize::MAX as usize).contains(&length) {
            return Err(invalid("invalid SHM region length"));
        }
        let mut mapping = Self::map(file, length, false, 0)?;
        mapping.validate_header()?;
        if mapping.kind == DATA && !seals.intersects(WRITE_SEALS) {
            return Err(invalid("SHM data region is not write-sealed"));
        }
        if mapping.kind == LEASES {
            // SAFETY: only the lease table becomes writable. Its received FD
            // and stable size-sealed mapping remain owned by this object.
            unsafe {
                rustix::mm::mprotect(
                    mapping.address.as_ptr().cast(),
                    length,
                    MprotectFlags::READ | MprotectFlags::WRITE,
                )?;
            }
            mapping.writable = true;
        }
        Ok(mapping)
    }

    #[cfg(miri)]
    pub(crate) fn import(_file: File) -> io::Result<Self> {
        Err(io::ErrorKind::Unsupported.into())
    }

    #[cfg(not(miri))]
    fn validate_header(&mut self) -> io::Result<()> {
        let header = self.bytes(0, LENGTH_OFFSET + std::mem::size_of::<u64>());
        let token = read_u64(header, TOKEN_OFFSET);
        let kind = read_u64(header, KIND_OFFSET);
        if &header[..REGION_MAGIC.len()] != REGION_MAGIC
            || !matches!(kind, DATA | LEASES)
            || read_u64(header, LENGTH_OFFSET) != self.length as u64
        {
            return Err(invalid("SHM region identity mismatch"));
        }
        self.id = u32::try_from(read_u64(
            self.bytes(ID_OFFSET, std::mem::size_of::<u64>()),
            0,
        ))
        .map_err(|_| invalid("invalid SHM region identifier"))?;
        self.token = token;
        self.kind = kind;
        Ok(())
    }

    #[cfg(not(miri))]
    fn map(file: File, length: usize, writable: bool, token: u64) -> io::Result<Self> {
        let protection = ProtFlags::READ
            | if writable {
                ProtFlags::WRITE
            } else {
                ProtFlags::empty()
            };
        // SAFETY: sealed file length has been checked; offset zero is aligned.
        let address = unsafe {
            rustix::mm::mmap(
                std::ptr::null_mut(),
                length,
                protection,
                MapFlags::SHARED,
                &file,
                0,
            )?
        };
        let id = file.as_raw_fd() as u32;
        Ok(Self {
            file,
            address: NonNull::new(address.cast()).expect("mmap returned non-null address"),
            length,
            writable,
            token,
            id,
            kind: 0,
            reservation: None,
        })
    }

    #[cfg_attr(
        miri,
        expect(clippy::unused_self, reason = "Miri cannot transfer descriptors.")
    )]
    pub(crate) fn descriptor(&self) -> io::Result<File> {
        #[cfg(not(miri))]
        {
            self.file.try_clone()
        }
        #[cfg(miri)]
        {
            Err(io::ErrorKind::Unsupported.into())
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.length
    }

    pub(crate) fn address(&self) -> usize {
        self.address.as_ptr() as usize
    }

    pub(crate) fn contains(&self, offset: usize, length: usize) -> bool {
        offset >= HEADER && offset <= self.length && length <= self.length - offset
    }

    pub(crate) fn bytes(&self, offset: usize, length: usize) -> &[u8] {
        assert!(offset <= self.length && length <= self.length - offset);
        // SAFETY: bounded immutable payload range. Callers never request
        // atomic cells as byte slices while those cells may change.
        unsafe { std::slice::from_raw_parts(self.address.as_ptr().add(offset), length) }
    }

    /// Caller must own exclusive access to this disjoint payload range.
    unsafe fn bytes_mut(&mut self, offset: usize, length: usize) -> &mut [u8] {
        assert!(self.writable && offset <= self.length && length <= self.length - offset);
        // SAFETY: the caller guarantees exclusivity; the range is checked.
        unsafe { std::slice::from_raw_parts_mut(self.address.as_ptr().add(offset), length) }
    }

    /// Caller has exclusive access to this disjoint writable range.
    #[expect(
        clippy::mut_from_ref,
        reason = "Exclusive ranges are enforced by pool leases or SPSC carrier cursors."
    )]
    unsafe fn slot_mut(&self, offset: usize, length: usize) -> &mut [u8] {
        assert!(
            self.writable
                && offset >= HEADER
                && offset <= self.length
                && length <= self.length - offset
        );
        // SAFETY: the caller's pool checkout or SPSC cursor ownership excludes
        // every reader before obtaining mutable access to this range.
        unsafe { std::slice::from_raw_parts_mut(self.address.as_ptr().add(offset), length) }
    }

    pub(super) fn write_carrier(&self, offset: usize, bytes: &[u8]) {
        // SAFETY: the connection's sole writer owns this free SPSC range after
        // acquiring the reader cursor; it publishes only after this copy.
        unsafe { self.slot_mut(offset, bytes.len()) }.copy_from_slice(bytes);
    }

    #[expect(
        clippy::cast_ptr_alignment,
        reason = "mmap is page aligned and the offset is checked below."
    )]
    pub(crate) fn atomic64(&self, offset: usize) -> &AtomicU64 {
        assert!(self.writable && offset.is_multiple_of(8) && offset <= self.length - 8);
        // SAFETY: aligned atomic-only storage in a writable shared mapping.
        unsafe { &*self.address.as_ptr().add(offset).cast::<AtomicU64>() }
    }

    #[expect(
        clippy::cast_ptr_alignment,
        reason = "mmap is page aligned and the offset is checked below."
    )]
    pub(crate) fn atomic32(&self, offset: usize) -> &AtomicU32 {
        assert!(self.writable && offset.is_multiple_of(4) && offset <= self.length - 4);
        // SAFETY: aligned atomic-only storage in a writable shared mapping.
        unsafe { &*self.address.as_ptr().add(offset).cast::<AtomicU32>() }
    }

    /// Try a fresh writable shared mapping of `file`, unmapping it on success.
    #[cfg(all(test, not(miri)))]
    fn try_map_writable(file: &File, length: usize) -> io::Result<()> {
        // SAFETY: a fresh mapping that nothing references; unmapped below.
        let address = unsafe {
            rustix::mm::mmap(
                std::ptr::null_mut(),
                length,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::SHARED,
                file,
                0,
            )?
        };
        // SAFETY: the mapping created above.
        unsafe { rustix::mm::munmap(address, length)? };
        Ok(())
    }

    /// Try to upgrade this mapping to writable.
    #[cfg(all(test, not(miri)))]
    fn try_mprotect_writable(&self) -> io::Result<()> {
        // SAFETY: changes only the protection of this object's own mapping.
        unsafe {
            rustix::mm::mprotect(
                self.address.as_ptr().cast(),
                self.length,
                MprotectFlags::READ | MprotectFlags::WRITE,
            )?;
        }
        Ok(())
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this object owns exactly this mapping. Borrowed payloads
        // retain an Arc to it; no reference survives the final Arc drop.
        #[cfg(not(miri))]
        let _ = unsafe { rustix::mm::munmap(self.address.as_ptr().cast(), self.length) };
        #[cfg(miri)]
        // SAFETY: the allocation uses this exact layout and has no surviving owner.
        unsafe {
            std::alloc::dealloc(self.address.as_ptr(), self.allocation);
        };
        drop(self.reservation.take());
    }
}

impl AsMut<[u8]> for crate::pool::SlotStorage {
    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: PayloadPool grants mutable checkout only after local
        // uniqueness and reusable() acquiring every remote completion.
        // Slots in one mapping have disjoint ranges.
        unsafe { self.0.mapping.slot_mut(self.0.offset, self.0.size) }
    }
}

pub(crate) fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("validated record"),
    )
}

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(all(test, not(miri)))]
mod tests {
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
            let file = File::from(
                rustix::fs::memfd_create("omq-invalid", MemfdFlags::ALLOW_SEALING).unwrap(),
            );
            file.set_len((HEADER + 16) as u64).unwrap();
            rustix::fs::fcntl_add_seals(&file, seals).unwrap();
            assert_eq!(
                Mapping::import(file).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        for length in [0, HEADER - 1, HEADER, HEADER + 16] {
            let file = File::from(
                rustix::fs::memfd_create("omq-invalid", MemfdFlags::ALLOW_SEALING).unwrap(),
            );
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
        let file = File::from(
            rustix::fs::memfd_create("omq-unsealed", MemfdFlags::ALLOW_SEALING).unwrap(),
        );
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
}
