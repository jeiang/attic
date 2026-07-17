//! A small, generic in-process cache with lazy TTL-based expiry.
//!
//! This is intentionally minimal: a single `RwLock<HashMap<..>>` guarding
//! `(inserted_at, value)` pairs. Reads take a shared lock in the common case
//! (fresh hit) and only upgrade to an exclusive lock when an expired entry
//! needs to be evicted.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// A cache mapping `K` to `V`, where entries expire `ttl` after insertion.
///
/// There is no active eviction thread: expired entries are only removed
/// lazily, the next time they are looked up via [`TtlCache::get`].
#[derive(Debug, Default)]
pub(crate) struct TtlCache<K, V> {
    map: RwLock<HashMap<K, (Instant, V)>>,
}

impl<K, V> TtlCache<K, V>
where
    K: Eq + Hash,
{
    /// Creates an empty cache.
    pub(crate) fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }

    /// Returns a clone of the cached value for `key`, if present and
    /// inserted less than `ttl` ago.
    ///
    /// If the entry is present but stale, it's lazily evicted.
    pub(crate) fn get(&self, key: &K, ttl: Duration) -> Option<V>
    where
        V: Clone,
    {
        self.get_at(key, ttl, Instant::now())
    }

    /// Same as [`TtlCache::get`], but with an explicit "current time" so
    /// freshness logic can be unit-tested without relying on real elapsed
    /// wall-clock time.
    fn get_at(&self, key: &K, ttl: Duration, now: Instant) -> Option<V>
    where
        V: Clone,
    {
        // Common case: the entry (if any) is fresh, so a shared lock
        // suffices.
        {
            let map = self.map.read().expect("TtlCache lock poisoned");
            match map.get(key) {
                Some((inserted_at, value)) if now.duration_since(*inserted_at) < ttl => {
                    return Some(value.clone());
                }
                Some(_) => {
                    // Present but stale; fall through to evict under a
                    // write lock.
                }
                None => return None,
            }
        }

        let mut map = self.map.write().expect("TtlCache lock poisoned");
        if let Some((inserted_at, _)) = map.get(key) {
            if now.duration_since(*inserted_at) >= ttl {
                map.remove(key);
            }
        }
        None
    }

    /// Inserts or overwrites `key` with `value`, timestamped with the
    /// current instant (refreshing the TTL window).
    pub(crate) fn insert(&self, key: K, value: V) {
        self.map
            .write()
            .expect("TtlCache lock poisoned")
            .insert(key, (Instant::now(), value));
    }

    /// Unconditionally removes `key`, if present.
    pub(crate) fn invalidate(&self, key: &K) {
        self.map
            .write()
            .expect("TtlCache lock poisoned")
            .remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_within_ttl() {
        let cache: TtlCache<&str, i32> = TtlCache::new();
        let t0 = Instant::now();
        cache.map.write().unwrap().insert("a", (t0, 1));

        let now = t0 + Duration::from_secs(3);
        assert_eq!(cache.get_at(&"a", Duration::from_secs(5), now), Some(1));

        // A fresh hit must not have evicted the entry.
        assert!(cache.map.read().unwrap().contains_key("a"));
    }

    #[test]
    fn miss_after_ttl_evicts_entry() {
        let cache: TtlCache<&str, i32> = TtlCache::new();
        let t0 = Instant::now();
        cache.map.write().unwrap().insert("a", (t0, 1));

        let now = t0 + Duration::from_secs(10);
        assert_eq!(cache.get_at(&"a", Duration::from_secs(5), now), None);

        // The stale entry should have been evicted as a side effect.
        assert!(!cache.map.read().unwrap().contains_key("a"));
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache: TtlCache<&str, i32> = TtlCache::new();
        cache.insert("a", 1);
        cache.invalidate(&"a");
        assert_eq!(cache.get(&"a", Duration::from_secs(60)), None);
    }

    #[test]
    fn overwrite_refreshes_timestamp() {
        let cache: TtlCache<&str, i32> = TtlCache::new();
        let t0 = Instant::now();
        cache.map.write().unwrap().insert("a", (t0, 1));
        cache
            .map
            .write()
            .unwrap()
            .insert("a", (t0 + Duration::from_secs(100), 2));

        // 101s after t0, but only 1s after the refreshed timestamp: still
        // fresh under a 5s TTL, and reflects the newer value.
        let now = t0 + Duration::from_secs(101);
        assert_eq!(cache.get_at(&"a", Duration::from_secs(5), now), Some(2));
    }

    #[test]
    fn miss_for_absent_key() {
        let cache: TtlCache<&str, i32> = TtlCache::new();
        assert_eq!(cache.get(&"missing", Duration::from_secs(60)), None);
    }
}
