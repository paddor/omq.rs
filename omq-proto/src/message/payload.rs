use super::{MAX_INLINE_PAYLOAD, Payload, PayloadInner, PayloadOwner, SharedOwner};
use bytes::Bytes;

impl Payload {
    /// Creates an empty payload (zero bytes).
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: PayloadInner::Empty,
        }
    }

    /// Creates a payload from a single `Bytes` chunk. Zero copy.
    /// Retains its owner even when the chunk is empty.
    #[inline]
    pub fn from_bytes(b: Bytes) -> Self {
        Self {
            inner: PayloadInner::Single(b),
        }
    }

    #[inline]
    pub(super) fn from_compact_bytes(bytes: Bytes) -> Self {
        if bytes.len() <= MAX_INLINE_PAYLOAD {
            Self::from_slice(&bytes)
        } else {
            Self::from_bytes(bytes)
        }
    }

    /// Wrap bytes with a conservative bound on their retained allocation.
    /// `retained` must include the entire backing allocation, including bytes
    /// outside this view. Callers that cannot determine it must use
    /// [`from_bytes`](Self::from_bytes) instead.
    ///
    /// Panics when the bound is smaller than the visible payload.
    pub fn from_bytes_with_retained_size(bytes: Bytes, retained: usize) -> Self {
        assert!(retained >= bytes.len());
        Self {
            inner: PayloadInner::Accounted { bytes, retained },
        }
    }

    /// Conservative retained allocation size, excluding the `Payload` value.
    /// `None` means an opaque byte owner whose allocation cannot be measured.
    pub fn retained_size(&self) -> Option<usize> {
        match &self.inner {
            PayloadInner::Identity(identity) => Some(identity.retained_size()),
            PayloadInner::Empty | PayloadInner::Inline { .. } => Some(0),
            PayloadInner::Single(_) => None,
            // Include a conservative allowance for Bytes' shared owner and
            // adapters in addition to the backing buffer itself.
            PayloadInner::Accounted { retained, .. } => Some(retained.saturating_add(128)),
            PayloadInner::Shared(owner) => owner.retained_size(),
        }
    }

    pub(super) fn bound_storage(&mut self) {
        if self.retained_size().is_none() {
            crate::copy_stats::record(crate::copy_stats::Site::BoundStorage, self.len());
            let bytes = Bytes::copy_from_slice(self.as_slice());
            *self = Self::from_bytes_with_retained_size(bytes, self.len());
        }
    }

    /// Creates an inline payload by copying `src` into the struct.
    /// Panics if `src.len() > MAX_INLINE_PAYLOAD`.
    #[inline]
    pub(crate) fn inline(src: &[u8]) -> Self {
        debug_assert!(src.len() <= MAX_INLINE_PAYLOAD);
        if src.is_empty() {
            return Self::new();
        }
        let mut data = [0u8; MAX_INLINE_PAYLOAD];
        data[..src.len()].copy_from_slice(src);
        Self {
            inner: PayloadInner::Inline {
                data,
                len: src.len() as u8,
            },
        }
    }

    /// Creates a payload by copying `src`: inline if it fits, heap otherwise.
    #[inline]
    pub fn from_slice(src: &[u8]) -> Self {
        if src.len() <= MAX_INLINE_PAYLOAD {
            Self::inline(src)
        } else {
            Self::from_bytes_with_retained_size(Bytes::copy_from_slice(src), src.len())
        }
    }

    /// Creates a payload from a static byte slice. Zero copy, zero alloc.
    pub fn from_static(b: &'static [u8]) -> Self {
        Self::from_bytes(Bytes::from_static(b))
    }

    /// Use an existing shared owner without allocating a new owner block.
    /// Empty owners remain retained. Converting this payload to `Bytes` allocates
    /// an adapter; borrowing or cloning the payload does not.
    #[inline]
    pub fn from_shared_owner(owner: std::sync::Arc<impl PayloadOwner>) -> Self {
        Self {
            inner: PayloadInner::Shared(SharedOwner::new(owner)),
        }
    }

    /// Borrow the first `len` bytes of a shared owner without allocating.
    /// The entire owner remains retained, including for an empty prefix.
    ///
    /// # Panics
    /// Panics when `len` exceeds the owner's byte length.
    #[inline]
    pub fn from_shared_owner_prefix(owner: std::sync::Arc<impl PayloadOwner>, len: usize) -> Self {
        Self {
            inner: PayloadInner::Shared(SharedOwner::prefix(owner, len)),
        }
    }

    /// Total payload length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.inner {
            PayloadInner::Identity(identity) => identity.as_ref().len(),
            PayloadInner::Empty => 0,
            PayloadInner::Inline { len, .. } => *len as usize,
            PayloadInner::Single(b) => b.len(),
            PayloadInner::Accounted { bytes, .. } => bytes.len(),
            PayloadInner::Shared(owner) => owner.len(),
        }
    }

    /// Whether the payload contains zero bytes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Zero-copy single-chunk borrow: `Some(&Bytes)` iff the payload holds
    /// exactly one `Bytes` chunk, even if empty. Returns `None` for inline,
    /// shared-owner, and default empty payloads.
    pub fn as_chunk(&self) -> Option<&Bytes> {
        match &self.inner {
            PayloadInner::Single(b) => Some(b),
            PayloadInner::Accounted { bytes, .. } => Some(bytes),
            _ => None,
        }
    }

    /// Returns the payload as a single contiguous `Bytes`.
    ///
    /// - Empty: `Bytes::new()`.
    /// - Inline: `Bytes::copy_from_slice` (<= 62 B copy).
    /// - Single: `Bytes::clone` (Arc bump only).
    /// - Shared owner: allocates one `Bytes` adapter retaining that owner.
    pub fn as_bytes(&self) -> Bytes {
        match &self.inner {
            PayloadInner::Identity(identity) => identity.to_bytes(),
            PayloadInner::Empty => Bytes::new(),
            PayloadInner::Inline { data, len } => Bytes::copy_from_slice(&data[..*len as usize]),
            PayloadInner::Single(b) => b.clone(),
            PayloadInner::Accounted { bytes, .. } => bytes.clone(),
            PayloadInner::Shared(owner) => Bytes::from_owner(owner.clone()),
        }
    }

    /// Borrow as a contiguous byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            PayloadInner::Identity(identity) => identity.as_ref(),
            PayloadInner::Empty => &[],
            PayloadInner::Inline { data, len } => &data[..*len as usize],
            PayloadInner::Single(b) => b,
            PayloadInner::Accounted { bytes, .. } => bytes,
            PayloadInner::Shared(owner) => owner.as_ref(),
        }
    }
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Payload {}

impl Default for Payload {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Payload").field(&self.as_slice()).finish()
    }
}

impl From<Bytes> for Payload {
    fn from(b: Bytes) -> Self {
        Self::from_bytes(b)
    }
}

impl From<&'static [u8]> for Payload {
    fn from(b: &'static [u8]) -> Self {
        Self::from_static(b)
    }
}

impl From<&'static str> for Payload {
    fn from(s: &'static str) -> Self {
        Self::from_static(s.as_bytes())
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Self::from_bytes(Bytes::from(v))
    }
}

impl From<String> for Payload {
    fn from(s: String) -> Self {
        Self::from_bytes(Bytes::from(s))
    }
}

impl From<Payload> for Bytes {
    /// Equivalent to `payload.as_bytes()`. Clones an existing Bytes chunk,
    /// copies inline data, or allocates a shared-owner adapter without copying.
    fn from(p: Payload) -> Bytes {
        p.as_bytes()
    }
}
