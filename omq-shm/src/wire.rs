//! Shared layouts for carrier records and completion coordinates.
#![forbid(unsafe_code)]

use std::io;

use crate::mapping::{invalid, read_u64};

pub(crate) const PREFACE_MAGIC: &[u8; 8] = b"OMQSHM08";
pub(crate) const COPIED: u8 = 0;
pub(crate) const BODY: u8 = 1;
pub(crate) const COPIED_BATCH: u8 = 2;
pub(crate) const REFERENCE_BATCH: u8 = 3;
pub(crate) const MULTIPART: u8 = 4;
pub(crate) use crate::coordination::{GENERATION_STEP, STATE_BITS};
pub(crate) const CELL_FIELDS: usize = crate::coordination::COORDINATES + 1;
pub(crate) const CELL_BYTES: usize = CELL_FIELDS * std::mem::size_of::<u64>();
pub(crate) const READER_CLOSED: usize = 32;
pub(crate) const WAKE_SEQUENCE: usize = 40;
pub(crate) const WAITER_PARKED: usize = 44;
pub(crate) const REGION_ID: usize = 1;
pub(crate) const REGION_TOKEN: usize = 2;
pub(crate) const BODY_OFFSET: usize = 3;
pub(crate) const BODY_LENGTH: usize = 4;
pub(crate) const RECORD_KIND: usize = 5;

const PREFACE_ID: usize = PREFACE_MAGIC.len();
const PREFACE_COUNT: usize = PREFACE_ID + std::mem::size_of::<u32>();
const PREFACE_TOKEN: usize = PREFACE_COUNT + std::mem::size_of::<u32>();

#[derive(Debug)]
pub(crate) struct Preface {
    pub(crate) id: u32,
    pub(crate) count: usize,
    pub(crate) token: u64,
}

impl Preface {
    pub(crate) fn encode(&self) -> [u8; crate::PREFACE_SIZE] {
        let mut bytes = [0; crate::PREFACE_SIZE];
        bytes[..PREFACE_ID].copy_from_slice(PREFACE_MAGIC);
        bytes[PREFACE_ID..PREFACE_COUNT].copy_from_slice(&self.id.to_le_bytes());
        bytes[PREFACE_COUNT..PREFACE_TOKEN].copy_from_slice(&(self.count as u32).to_le_bytes());
        bytes[PREFACE_TOKEN..].copy_from_slice(&self.token.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8; crate::PREFACE_SIZE]) -> io::Result<Self> {
        let count = u32::from_le_bytes(
            bytes[PREFACE_COUNT..PREFACE_TOKEN]
                .try_into()
                .expect("preface count"),
        ) as usize;
        if &bytes[..PREFACE_ID] != PREFACE_MAGIC || !(1..=crate::MAX_LEASES).contains(&count) {
            return Err(invalid("invalid SHM preface"));
        }
        Ok(Self {
            id: u32::from_le_bytes(
                bytes[PREFACE_ID..PREFACE_COUNT]
                    .try_into()
                    .expect("preface id"),
            ),
            count,
            token: read_u64(bytes, PREFACE_TOKEN),
        })
    }
}

const KIND: usize = 0;
const ID: usize = KIND + 1;
const TOKEN: usize = ID + std::mem::size_of::<u32>();
const OFFSET: usize = TOKEN + std::mem::size_of::<u64>();
const LENGTH: usize = OFFSET + std::mem::size_of::<u64>();
const LEASE: usize = LENGTH + std::mem::size_of::<u64>();
const GENERATION: usize = LEASE + std::mem::size_of::<u32>();
pub(crate) const REFERENCE_SIZE: usize = GENERATION + std::mem::size_of::<u64>();

#[derive(Clone, Copy, Debug)]
pub(crate) struct Reference {
    pub(crate) kind: u8,
    pub(crate) id: u32,
    pub(crate) token: u64,
    pub(crate) offset: usize,
    pub(crate) length: usize,
    pub(crate) lease: usize,
    pub(crate) generation: u64,
}

impl Reference {
    pub(crate) fn encode(self) -> [u8; REFERENCE_SIZE] {
        let mut bytes = [0; REFERENCE_SIZE];
        bytes[KIND] = self.kind;
        bytes[ID..TOKEN].copy_from_slice(&self.id.to_le_bytes());
        bytes[TOKEN..OFFSET].copy_from_slice(&self.token.to_le_bytes());
        bytes[OFFSET..LENGTH].copy_from_slice(&(self.offset as u64).to_le_bytes());
        bytes[LENGTH..LEASE].copy_from_slice(&(self.length as u64).to_le_bytes());
        bytes[LEASE..GENERATION].copy_from_slice(&(self.lease as u32).to_le_bytes());
        bytes[GENERATION..].copy_from_slice(&self.generation.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], kind: u8) -> io::Result<Self> {
        if bytes.len() != REFERENCE_SIZE || bytes.first() != Some(&kind) {
            return Err(invalid("invalid SHM body record"));
        }
        Ok(Self {
            kind,
            id: u32::from_le_bytes(bytes[ID..TOKEN].try_into().expect("reference id")),
            token: read_u64(bytes, TOKEN),
            offset: usize::try_from(read_u64(bytes, OFFSET))
                .map_err(|_| invalid("SHM offset overflow"))?,
            length: usize::try_from(read_u64(bytes, LENGTH))
                .map_err(|_| invalid("SHM length overflow"))?,
            lease: u32::from_le_bytes(bytes[LEASE..GENERATION].try_into().expect("lease")) as usize,
            generation: read_u64(bytes, GENERATION),
        })
    }

    pub(crate) fn coordinates(self) -> [u64; CELL_FIELDS - 1] {
        [
            u64::from(self.id),
            self.token,
            self.offset as u64,
            self.length as u64,
            u64::from(self.kind),
        ]
    }
}
