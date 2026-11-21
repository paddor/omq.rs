//! Listener admission by exact remote IP address.

use std::net::IpAddr;
use std::sync::Arc;

use crate::error::{Error, Result};

/// Exact remote IP addresses a listener admits.
///
/// Matching ignores the port. IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`)
/// match their IPv4 form, in the list and in peer addresses. An empty list
/// admits no peer. Clones share one sorted address table.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct IpAllowlist {
    /// Canonical, sorted, and free of duplicates.
    addresses: Arc<[IpAddr]>,
}

impl IpAllowlist {
    /// Whether a peer at `address` is admitted.
    #[must_use]
    pub fn contains(&self, address: IpAddr) -> bool {
        self.addresses
            .binary_search(&address.to_canonical())
            .is_ok()
    }

    /// Number of distinct addresses.
    #[must_use]
    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    /// Whether the list admits no peer.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }

    /// Admitted addresses in canonical form, sorted.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = IpAddr> + '_ {
        self.addresses.iter().copied()
    }
}

impl FromIterator<IpAddr> for IpAllowlist {
    fn from_iter<I: IntoIterator<Item = IpAddr>>(addresses: I) -> Self {
        let mut addresses: Vec<IpAddr> = addresses.into_iter().map(|a| a.to_canonical()).collect();
        addresses.sort_unstable();
        addresses.dedup();

        Self {
            addresses: addresses.into(),
        }
    }
}

impl<S: AsRef<str>> TryFrom<&[S]> for IpAllowlist {
    type Error = Error;

    /// Parse IP literals, such as `10.0.0.7` or `fd00::7`. Surrounding
    /// whitespace is ignored. Ports, brackets, zone IDs, prefixes, and host
    /// names are rejected.
    ///
    /// # Errors
    /// Returns [`Error::Config`] naming the first invalid entry. Nothing is
    /// built then.
    fn try_from(entries: &[S]) -> Result<Self> {
        entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let entry = entry.as_ref();
                entry.trim().parse::<IpAddr>().map_err(|_| {
                    Error::Config(format!(
                        "IP allowlist entry {index} ({entry:?}) is not an IP address"
                    ))
                })
            })
            .collect()
    }
}
