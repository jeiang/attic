//! Cache configuration endpoint.

use serde::{Deserialize, Serialize};

use crate::cache::CacheName;
use crate::signing::NixKeypair;

/// List of caches discoverable by the caller.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListCachesResponse {
    /// Cache names, sorted lexicographically.
    pub caches: Vec<CacheName>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCacheRequest {
    /// The keypair of the cache.
    pub keypair: KeypairConfig,

    /// Whether the cache is public or not.
    ///
    /// Anonymous clients are implicitly granted the "pull"
    /// permission to public caches.
    pub is_public: bool,

    /// The Nix store path this binary cache uses.
    ///
    /// This is usually `/nix/store`.
    pub store_dir: String,

    /// The priority of the binary cache.
    ///
    /// A lower number denotes a higher priority.
    /// <https://cache.nixos.org> has a priority of 40.
    pub priority: i32,

    /// A list of signing key names of upstream caches.
    ///
    /// The list serves as a hint to clients to avoid uploading
    /// store paths signed with such keys.
    pub upstream_cache_key_names: Vec<String>,
}

/// Configuration of a cache.
///
/// Specifying `None` means using the default value or
/// keeping the current value.
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheConfig {
    /// The keypair of the cache.
    ///
    /// The keypair is never returned by the server, but can
    /// be configured by the client.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keypair: Option<KeypairConfig>,

    /// The Nix binary cache endpoint of the cache.
    ///
    /// This is the endpoint that should be added to `nix.conf`.
    /// This is read-only and may not be available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub substituter_endpoint: Option<String>,

    /// The Attic API endpoint.
    ///
    /// This is read-only and may not be available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_endpoint: Option<String>,

    /// The public key of the cache, in the canonical format used by Nix.
    ///
    /// This is read-only and may not be available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key: Option<String>,

    /// Whether the cache is public or not.
    ///
    /// Anonymous clients are implicitly granted the "pull"
    /// permission to public caches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_public: Option<bool>,

    /// The Nix store path this binary cache uses.
    ///
    /// This is usually `/nix/store`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_dir: Option<String>,

    /// The priority of the binary cache.
    ///
    /// A lower number denotes a higher priority.
    /// <https://cache.nixos.org> has a priority of 40.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// A list of signing key names of upstream caches.
    ///
    /// The list serves as a hint to clients to avoid uploading
    /// store paths signed with such keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_cache_key_names: Option<Vec<String>>,

    /// The retention period of the cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_period: Option<RetentionPeriodConfig>,

    /// The chunking parameters advertised by the server.
    ///
    /// This is read-only and may not be available.
    ///
    /// Its presence also signals that the server supports chunk-level
    /// dedup negotiation (the `get-missing-chunks` endpoint, and chunk
    /// manifests on path uploads). It is absent when chunking is disabled
    /// on the server, or when the server requires proof of possession of
    /// uploaded data (proof of possession requires the client to actually
    /// send the NAR bytes, which is incompatible with letting clients skip
    /// chunks the server already has).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub chunking: Option<ChunkingParameters>,
}

/// Chunking parameters used by the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingParameters {
    /// The minimum NAR size to trigger chunking.
    #[serde(rename = "nar-size-threshold")]
    pub nar_size_threshold: usize,

    /// The preferred minimum size of a chunk, in bytes.
    #[serde(rename = "min-size")]
    pub min_size: usize,

    /// The preferred average size of a chunk, in bytes.
    #[serde(rename = "avg-size")]
    pub avg_size: usize,

    /// The preferred maximum size of a chunk, in bytes.
    #[serde(rename = "max-size")]
    pub max_size: usize,
}

/// Configuaration of a keypair.
#[derive(Debug, Serialize, Deserialize)]
pub enum KeypairConfig {
    /// Use a randomly-generated keypair.
    Generate,

    /// Use a client-specified keypair.
    Keypair(NixKeypair),
}

/// Configuration of retention period.
#[derive(Debug, Serialize, Deserialize)]
pub enum RetentionPeriodConfig {
    /// Use the global default.
    Global,

    /// Specify a retention period in seconds.
    ///
    /// If 0, then time-based garbage collection is disabled.
    Period(u32),
}

impl CacheConfig {
    pub fn blank() -> Self {
        Self {
            keypair: None,
            substituter_endpoint: None,
            api_endpoint: None,
            public_key: None,
            is_public: None,
            store_dir: None,
            priority: None,
            upstream_cache_key_names: None,
            retention_period: None,
            chunking: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunking_parameters_round_trips_through_json() {
        let params = ChunkingParameters {
            nar_size_threshold: 128 * 1024,
            min_size: 16 * 1024,
            avg_size: 64 * 1024,
            max_size: 256 * 1024,
        };

        let json = serde_json::to_string(&params).unwrap();
        let deserialized: ChunkingParameters = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.nar_size_threshold, params.nar_size_threshold);
        assert_eq!(deserialized.min_size, params.min_size);
        assert_eq!(deserialized.avg_size, params.avg_size);
        assert_eq!(deserialized.max_size, params.max_size);
    }

    /// Old clients/servers that don't know about `chunking` must still be
    /// able to deserialize a `CacheConfig` that lacks the field, and it
    /// must default to `None` rather than failing to deserialize.
    #[test]
    fn cache_config_without_chunking_field_defaults_to_none() {
        let json = r#"{}"#;
        // Note: all other fields are also `Option` with `skip_serializing_if`
        // but only `chunking` carries an explicit `#[serde(default)]` (added
        // alongside this field); this asserts that an entirely absent
        // `chunking` key still deserializes successfully.
        let deserialized: CacheConfig = serde_json::from_str(json).unwrap();
        assert!(deserialized.chunking.is_none());
    }

    #[test]
    fn cache_config_with_chunking_round_trips_through_json() {
        let mut config = CacheConfig::blank();
        config.chunking = Some(ChunkingParameters {
            nar_size_threshold: 128 * 1024,
            min_size: 16 * 1024,
            avg_size: 64 * 1024,
            max_size: 256 * 1024,
        });

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CacheConfig = serde_json::from_str(&json).unwrap();

        let chunking = deserialized.chunking.expect("chunking present");
        assert_eq!(chunking.nar_size_threshold, 128 * 1024);
    }
}
