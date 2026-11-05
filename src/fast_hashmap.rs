//! A `HashMap` with `u64` keys that skips hashing.
//!
//! [`FastU64Map`] uses each key as its own hash value (via
//! [`nohash_hasher::BuildNoHashHasher`]), so lookups avoid the cost of the
//! default SipHash hasher.
//!
//! This works best when keys are already spread out, such as IDs or counters.
//! It gives no protection against HashDoS, so don't use it with keys an
//! attacker can choose.

// ============================================================================
// Import Definition
// ============================================================================

use nohash_hasher::BuildNoHashHasher;
use std::{
    collections::{HashMap, hash_map},
    ops::{Deref, DerefMut},
};

// ============================================================================
// Fast u64 Map Definition
// ============================================================================

/// A `HashMap<u64, V>` that uses each key as its own hash.
///
/// Derefs to the inner [`HashMap`], so all the usual `HashMap` methods work.
///
/// ```
/// use adaptif_gatotkaca_utils::FastU64Map;
///
/// let mut map = FastU64Map::with_capacity(16);
/// map.insert(7, "seven");
/// assert_eq!(map.get(&7), Some(&"seven"));
/// ```
#[derive(Debug, Clone)]
pub struct FastU64Map<V>(HashMap<u64, V, BuildNoHashHasher<u64>>);

/// Alias of [`FastU64Map`] for maps keyed by SQLite row IDs.
pub type SqliteFastU64Map<V> = FastU64Map<V>;

impl<V> Default for FastU64Map<V> {
    /// Creates an empty map. Same as [`FastU64Map::new`].
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl<V> FastU64Map<V> {
    /// Creates an empty map.
    #[inline(always)]
    pub fn new() -> Self {
        Self(HashMap::with_hasher(BuildNoHashHasher::default()))
    }

    /// Creates an empty map with room for at least `capacity` entries.
    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self(HashMap::with_capacity_and_hasher(
            capacity,
            BuildNoHashHasher::default(),
        ))
    }

    /// Returns the inner `HashMap`.
    #[inline(always)]
    pub fn into_inner(self) -> HashMap<u64, V, BuildNoHashHasher<u64>> {
        self.0
    }
}

impl<V> Deref for FastU64Map<V> {
    type Target = HashMap<u64, V, BuildNoHashHasher<u64>>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V> DerefMut for FastU64Map<V> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<V> IntoIterator for FastU64Map<V> {
    type Item = (u64, V);
    type IntoIter = hash_map::IntoIter<u64, V>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, V> IntoIterator for &'a FastU64Map<V> {
    type Item = (&'a u64, &'a V);
    type IntoIter = hash_map::Iter<'a, u64, V>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, V> IntoIterator for &'a mut FastU64Map<V> {
    type Item = (&'a u64, &'a mut V);
    type IntoIter = hash_map::IterMut<'a, u64, V>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}
