use std::collections::{HashMap, HashSet};

use axum::extract::{Extension, Json};
use sea_orm::entity::prelude::*;
use sea_orm::{FromQueryResult, QuerySelect};
use tracing::instrument;

use crate::database::entity::chunk::{self, ChunkState, Entity as Chunk};
use crate::error::{ServerError, ServerResult};
use crate::narinfo::Compression;
use crate::{RequestState, State};
use attic::api::v1::get_missing_chunks::{GetMissingChunksRequest, GetMissingChunksResponse};
use attic::hash::Hash;

#[derive(FromQueryResult)]
struct ChunkHashOnly {
    chunk_hash: String,
}

/// Gets information on missing chunks in a cache.
///
/// Requires "push" permission as it essentially allows probing
/// of cache contents.
#[instrument(skip_all, fields(payload))]
pub(crate) async fn get_missing_chunks(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Json(payload): Json<GetMissingChunksRequest>,
) -> ServerResult<Json<GetMissingChunksResponse>> {
    let database = state.database().await?;
    req_state
        .auth
        .auth_cache(&state, &payload.cache, |_, permission| {
            permission.require_push()?;
            Ok(())
        })
        .await?;

    // Only chunks compressed with the server's currently-configured
    // compression are directly reusable without recompression, so a chunk
    // stored under a different compression doesn't count as "present" for
    // negotiation purposes.
    let compression: Compression = state.config.compression.r#type.into();

    // Deduped mapping from the on-wire (typed-base16) hash string to the
    // original `Hash`, so we can avoid re-parsing hashes out of the
    // database rows below.
    let requested_hashes: HashMap<String, Hash> = payload
        .chunk_hashes
        .into_iter()
        .map(|h| (h.to_typed_base16(), h))
        .collect();

    let query_in = requested_hashes.keys().map(|h| Value::from(h.to_owned()));

    let result: Vec<ChunkHashOnly> = Chunk::find()
        .select_only()
        .column_as(chunk::Column::ChunkHash, "chunk_hash")
        .filter(chunk::Column::State.eq(ChunkState::Valid))
        .filter(chunk::Column::Compression.eq(compression.as_str()))
        .filter(chunk::Column::ChunkHash.is_in(query_in))
        .into_model::<ChunkHashOnly>()
        .all(database)
        .await
        .map_err(ServerError::database_error)?;

    let found_hashes: HashSet<String> = result.into_iter().map(|row| row.chunk_hash).collect();

    let missing_chunks = requested_hashes
        .into_iter()
        .filter(|(hash_str, _)| !found_hashes.contains(hash_str))
        .map(|(_, hash)| hash)
        .collect();

    Ok(Json(GetMissingChunksResponse { missing_chunks }))
}
