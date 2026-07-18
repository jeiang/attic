use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_with::{DefaultOnError, serde_as};

use crate::cache::CacheName;
use crate::hash::Hash;
use crate::nix_store::StorePathHash;

/// Header containing the upload info.
pub const ATTIC_NAR_INFO: &str = "X-Attic-Nar-Info";

/// Header containing the size of the upload info at the beginning of the body.
pub const ATTIC_NAR_INFO_PREAMBLE_SIZE: &str = "X-Attic-Nar-Info-Preamble-Size";

/// NAR information associated with a upload.
///
/// There are two ways for the client to supply the NAR information:
///
/// 1. At the beginning of the PUT body. The `X-Attic-Nar-Info-Preamble-Size`
///    header must be set to the size of the JSON.
/// 2. Through the `X-Attic-Nar-Info` header.
///
/// The client is advised to use the first method if the serialized
/// JSON is large (>4K).
///
/// Regardless of client compression, the server will always decompress
/// the NAR to validate the NAR hash before applying the server-configured
/// compression again.
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadPathNarInfo {
    /// The name of the binary cache to upload to.
    pub cache: CacheName,

    /// The hash portion of the store path.
    pub store_path_hash: StorePathHash,

    /// The full store path being cached, including the store directory.
    pub store_path: String,

    /// Other store paths this object directly refereces.
    pub references: Vec<String>,

    /// The system this derivation is built for.
    pub system: Option<String>,

    /// The derivation that produced this object.
    pub deriver: Option<String>,

    /// The signatures of this object.
    pub sigs: Vec<String>,

    /// The CA field of this object.
    pub ca: Option<String>,

    /// The hash of the NAR.
    ///
    /// It must begin with `sha256:` with the SHA-256 hash in the
    /// hexadecimal format (64 hex characters).
    ///
    /// This is informational and the server always validates the supplied
    /// hash.
    pub nar_hash: Hash,

    /// The size of the NAR.
    pub nar_size: usize,

    /// The chunk manifest for a negotiated (dedup-aware) upload.
    ///
    /// When present, this describes the NAR's content-defined chunks in
    /// NAR stream order. The entries' sizes must sum to `nar_size`.
    ///
    /// The request body then consists of exactly the raw, uncompressed
    /// bytes of the `inline` entries' chunk data, concatenated in the same
    /// order as they appear in the manifest — entries the client believes
    /// the server already has (e.g. from a prior `get-missing-chunks`
    /// call) are marked `inline: false` and are omitted from the body
    /// entirely.
    ///
    /// If the same chunk hash appears more than once in the manifest, only
    /// the first occurrence that actually needs uploading may be `inline`;
    /// every later occurrence of that hash must be a reference
    /// (`inline: false`). This is safe because the server processes
    /// entries sequentially in stream order, so the inline upload of the
    /// first occurrence has already completed and made the chunk available
    /// by the time later references to it are resolved.
    ///
    /// Every non-inline (referenced) entry must already exist on the
    /// server, or the whole upload fails — the client should treat this as
    /// a signal to renegotiate (re-run `get-missing-chunks`) or fall back
    /// to a full, non-negotiated upload.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub chunk_manifest: Option<Vec<ChunkManifestEntry>>,
}

/// One entry of a chunk manifest, in NAR stream order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkManifestEntry {
    /// SHA-256 hash of the uncompressed chunk data.
    pub hash: Hash,

    /// Size of the uncompressed chunk data in bytes.
    pub size: usize,

    /// Whether the chunk's raw data is included inline in the request body.
    pub inline: bool,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadPathResult {
    #[serde_as(deserialize_as = "DefaultOnError")]
    pub kind: UploadPathResultKind,

    /// The compressed size of the NAR, in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size: Option<usize>,

    /// The fraction of data that was deduplicated, from 0 to 1.
    pub frac_deduplicated: Option<f64>,
}

/// Builds a chunk manifest from a NAR's ordered chunks and the set of
/// chunk hashes the server reported as missing.
///
/// This implements the "first-missing-occurrence-inline" rule documented
/// on [`UploadPathNarInfo::chunk_manifest`]: an entry is `inline` iff its
/// hash is in `missing` and no earlier entry in `chunks` with the same hash
/// has already been marked `inline`. This keeps repeated chunks (e.g. runs
/// of zeroes) from being uploaded more than once even when the whole
/// chunk is missing server-side.
///
/// `chunks` must be in NAR stream order; the returned manifest preserves
/// that order and each entry's size.
pub fn build_chunk_manifest(
    chunks: &[(Hash, usize)],
    missing: &HashSet<Hash>,
) -> Vec<ChunkManifestEntry> {
    let mut already_inlined: HashSet<&Hash> = HashSet::new();

    chunks
        .iter()
        .map(|(hash, size)| {
            let inline = missing.contains(hash) && already_inlined.insert(hash);

            ChunkManifestEntry {
                hash: hash.clone(),
                size: *size,
                inline,
            }
        })
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum UploadPathResultKind {
    /// The path was uploaded.
    ///
    /// This is purely informational and servers may return
    /// this variant even when the NAR is deduplicated.
    #[default]
    Uploaded,

    /// The path was globally deduplicated.
    ///
    /// The exact semantics of what counts as deduplicated
    /// is opaque to the client.
    Deduplicated,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_upload_path_nar_info() -> UploadPathNarInfo {
        UploadPathNarInfo {
            cache: CacheName::new("test".to_string()).unwrap(),
            store_path_hash: StorePathHash::new("ia70ss13m22znbl8khrf2hq72qmh5drr".to_string())
                .unwrap(),
            store_path: "/nix/store/ia70ss13m22znbl8khrf2hq72qmh5drr-foo".to_string(),
            references: Vec::new(),
            system: None,
            deriver: None,
            sigs: Vec::new(),
            ca: None,
            nar_hash: Hash::sha256_from_bytes(b"test"),
            nar_size: 4,
            chunk_manifest: None,
        }
    }

    #[test]
    fn chunk_manifest_entry_round_trips_through_json() {
        let entry = ChunkManifestEntry {
            hash: Hash::sha256_from_bytes(b"chunk"),
            size: 1234,
            inline: true,
        };

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: ChunkManifestEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.hash, entry.hash);
        assert_eq!(deserialized.size, entry.size);
        assert_eq!(deserialized.inline, entry.inline);
    }

    #[test]
    fn upload_path_nar_info_with_chunk_manifest_round_trips_through_json() {
        let mut info = sample_upload_path_nar_info();
        info.chunk_manifest = Some(vec![
            ChunkManifestEntry {
                hash: Hash::sha256_from_bytes(b"chunk-a"),
                size: 100,
                inline: true,
            },
            ChunkManifestEntry {
                hash: Hash::sha256_from_bytes(b"chunk-b"),
                size: 200,
                inline: false,
            },
        ]);

        let json = serde_json::to_string(&info).unwrap();
        let deserialized: UploadPathNarInfo = serde_json::from_str(&json).unwrap();

        let manifest = deserialized.chunk_manifest.expect("manifest present");
        assert_eq!(manifest.len(), 2);
        assert_eq!(manifest[0].size, 100);
        assert!(manifest[0].inline);
        assert_eq!(manifest[1].size, 200);
        assert!(!manifest[1].inline);
    }

    /// Old clients/servers that don't know about `chunk_manifest` must
    /// still be able to deserialize (or produce, on the wire) an
    /// `UploadPathNarInfo` without the field, and it must default to
    /// `None` rather than failing to deserialize.
    #[test]
    fn upload_path_nar_info_without_chunk_manifest_field_defaults_to_none() {
        let info = sample_upload_path_nar_info();

        // Serialize, then manually strip the `chunk_manifest` key to
        // simulate an old client/server that never sent it.
        let value = serde_json::to_value(&info).unwrap();
        let mut map = value.as_object().unwrap().clone();
        assert!(
            !map.contains_key("chunk_manifest"),
            "chunk_manifest should be omitted by skip_serializing_if when None"
        );
        map.remove("chunk_manifest");
        let value = serde_json::Value::Object(map);

        let deserialized: UploadPathNarInfo = serde_json::from_value(value).unwrap();
        assert!(deserialized.chunk_manifest.is_none());
    }

    fn chunk(seed: &[u8], size: usize) -> (Hash, usize) {
        (Hash::sha256_from_bytes(seed), size)
    }

    #[test]
    fn build_chunk_manifest_all_missing_inlines_everything_except_repeats() {
        let a = chunk(b"a", 10);
        let b = chunk(b"b", 20);
        let chunks = vec![a.clone(), b.clone(), a.clone()];
        let missing: HashSet<Hash> = [a.0.clone(), b.0.clone()].into_iter().collect();

        let manifest = build_chunk_manifest(&chunks, &missing);

        assert_eq!(manifest.len(), 3);
        assert_eq!(manifest[0].hash, a.0);
        assert_eq!(manifest[0].size, 10);
        assert!(manifest[0].inline);
        assert_eq!(manifest[1].hash, b.0);
        assert_eq!(manifest[1].size, 20);
        assert!(manifest[1].inline);
        // Repeated occurrence of an already-inlined hash must be a reference.
        assert_eq!(manifest[2].hash, a.0);
        assert_eq!(manifest[2].size, 10);
        assert!(!manifest[2].inline);
    }

    #[test]
    fn build_chunk_manifest_none_missing_is_all_references() {
        let a = chunk(b"a", 10);
        let b = chunk(b"b", 20);
        let chunks = vec![a.clone(), b.clone()];
        let missing: HashSet<Hash> = HashSet::new();

        let manifest = build_chunk_manifest(&chunks, &missing);

        assert_eq!(manifest.len(), 2);
        assert!(manifest.iter().all(|entry| !entry.inline));
        assert_eq!(manifest[0].size, 10);
        assert_eq!(manifest[1].size, 20);
    }

    #[test]
    fn build_chunk_manifest_repeated_missing_hash_only_first_occurrence_is_inline() {
        let a = chunk(b"a", 10);
        let b = chunk(b"b", 20);
        // a, b, a, a - only the first `a` should be inline.
        let chunks = vec![a.clone(), b.clone(), a.clone(), a.clone()];
        let missing: HashSet<Hash> = [a.0.clone()].into_iter().collect();

        let manifest = build_chunk_manifest(&chunks, &missing);

        assert_eq!(manifest.len(), 4);
        assert!(manifest[0].inline);
        assert!(!manifest[1].inline); // b was never missing
        assert!(!manifest[2].inline); // repeat of a, already inlined
        assert!(!manifest[3].inline); // repeat of a, already inlined
    }

    #[test]
    fn build_chunk_manifest_preserves_order_and_sizes() {
        let a = chunk(b"a", 111);
        let b = chunk(b"b", 222);
        let c = chunk(b"c", 333);
        let chunks = vec![c.clone(), a.clone(), b.clone()];
        let missing: HashSet<Hash> = [b.0.clone()].into_iter().collect();

        let manifest = build_chunk_manifest(&chunks, &missing);

        let sizes: Vec<usize> = manifest.iter().map(|entry| entry.size).collect();
        assert_eq!(sizes, vec![333, 111, 222]);
        let hashes: Vec<&Hash> = manifest.iter().map(|entry| &entry.hash).collect();
        assert_eq!(hashes, vec![&c.0, &a.0, &b.0]);
    }
}
