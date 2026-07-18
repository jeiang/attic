//! get-missing-chunks v1
//!
//! `POST /_api/v1/get-missing-chunks`
//!
//! Requires "push" permission.

use serde::{Deserialize, Serialize};

use crate::cache::CacheName;
use crate::hash::Hash;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetMissingChunksRequest {
    /// The name of the cache.
    pub cache: CacheName,

    /// The list of chunk hashes.
    pub chunk_hashes: Vec<Hash>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetMissingChunksResponse {
    /// A list of chunk hashes that are not in the cache.
    pub missing_chunks: Vec<Hash>,
}
