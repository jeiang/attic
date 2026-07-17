//! Nix Binary Cache server.
//!
//! This module implements the Nix Binary Cache API.
//!
//! The implementation is based on the specifications at <https://github.com/fzakaria/nix-http-binary-cache-api-spec>.

use std::collections::VecDeque;
use std::io::Error as IoError;
use std::path::PathBuf;
use std::sync::Arc;

use axum::http;
use axum::{
    Router,
    body::Body,
    extract::{Extension, Path},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use futures::TryStreamExt as _;
use futures::stream::BoxStream;
use serde::Serialize;
use tokio_util::io::ReaderStream;
use tracing::instrument;

use crate::database::AtticDatabase;
use crate::database::entity::chunk::ChunkModel;
use crate::error::{ErrorKind, ServerResult};
use crate::nix_manifest;
use crate::storage::{Download, StorageBackend, StorageBackendImpl};
use crate::{RequestState, State};
use attic::cache::CacheName;
use attic::io::merge_chunks;
use attic::mime;
use attic::nix_store::StorePathHash;

/// Buffer size for `ReaderStream`, whose 4 KiB default is too small for streaming multi-MiB NARs.
const STREAM_BUFFER_SIZE: usize = 64 * 1024;

/// Builds a `Cache-Control` header value.
///
/// `is_public` controls the cacheability directive: `public` caches (shared
/// HTTP caches, CDNs) may store responses for public Attic caches, while
/// `private` responses must only be cached by the requesting client since
/// they require an auth token to fetch. `immutable` should be set for
/// content-addressed payloads (NARs) that never change once fetched.
fn cache_control(is_public: bool, max_age_secs: u64, immutable: bool) -> http::HeaderValue {
    let visibility = if is_public { "public" } else { "private" };

    let value = if immutable {
        format!("{visibility}, max-age={max_age_secs}, immutable")
    } else {
        format!("{visibility}, max-age={max_age_secs}")
    };

    http::HeaderValue::from_str(&value).expect("Cache-Control value must be a valid header value")
}

/// How often `last_accessed_at` is allowed to be bumped for a given object.
///
/// The timestamp only feeds retention-based garbage collection, which
/// compares it against cutoffs measured in hours to days (see
/// `run_time_based_garbage_collection` in `gc.rs`). Bumps within this
/// window are therefore skipped: the extra precision has no observable
/// effect on GC decisions, but re-bumping on every single download would
/// put a database write on the critical path of every NAR fetch.
const LAST_ACCESSED_DEBOUNCE: chrono::Duration = chrono::Duration::hours(1);

/// Nix cache information.
///
/// An example of a correct response is as follows:
///
/// ```text
/// StoreDir: /nix/store
/// WantMassQuery: 1
/// Priority: 40
/// ```
#[derive(Debug, Clone, Serialize)]
struct NixCacheInfo {
    /// Whether this binary cache supports bulk queries.
    #[serde(rename = "WantMassQuery")]
    want_mass_query: bool,

    /// The Nix store path this binary cache uses.
    #[serde(rename = "StoreDir")]
    store_dir: PathBuf,

    /// The priority of the binary cache.
    ///
    /// A lower number denotes a higher priority.
    /// <https://cache.nixos.org> has a priority of 40.
    #[serde(rename = "Priority")]
    priority: i32,
}

impl IntoResponse for NixCacheInfo {
    fn into_response(self) -> Response {
        match nix_manifest::to_string(&self) {
            Ok(body) => Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", mime::NIX_CACHE_INFO)
                .body(body)
                .unwrap()
                .into_response(),
            Err(e) => e.into_response(),
        }
    }
}

/// Gets information on a cache.
#[instrument(skip_all, fields(cache_name))]
async fn get_nix_cache_info(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(cache_name): Path<CacheName>,
) -> ServerResult<Response> {
    let database = state.database().await?;
    let cache = req_state
        .auth
        .auth_cache(database, &cache_name, |cache, permission| {
            permission.require_pull()?;
            Ok(cache)
        })
        .await?;

    req_state.set_public_cache(cache.is_public);

    let info = NixCacheInfo {
        want_mass_query: true,
        store_dir: cache.store_dir.into(),
        priority: cache.priority,
    };

    let mut response = info.into_response();
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        cache_control(cache.is_public, 300, false),
    );

    Ok(response)
}

/// Gets various information on a store path hash.
///
/// `/:cache/:path`, which may be one of
/// - GET `/:cache/{storePathHash}.narinfo`
/// - HEAD `/:cache/{storePathHash}.narinfo`
/// - GET `/:cache/{storePathHash}.ls` (not implemented)
#[instrument(skip_all, fields(cache_name, path))]
#[axum_macros::debug_handler]
async fn get_store_path_info(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path((cache_name, path)): Path<(CacheName, String)>,
) -> ServerResult<Response> {
    let components: Vec<&str> = path.splitn(2, '.').collect();

    if components.len() != 2 {
        return Err(ErrorKind::NotFound.into());
    }

    // TODO: Other endpoints
    if components[1] != "narinfo" {
        return Err(ErrorKind::NotFound.into());
    }

    let store_path_hash = StorePathHash::new(components[0].to_string())?;

    tracing::debug!(
        "Received request for {}.narinfo in {:?}",
        store_path_hash.as_str(),
        cache_name
    );

    let (object, cache, nar, _) = state
        .database()
        .await?
        .find_object_and_chunks_by_store_path_hash(&cache_name, &store_path_hash, false)
        .await?;

    let permission = req_state
        .auth
        .get_permission_for_cache(&cache_name, cache.is_public);
    permission.require_pull()?;

    req_state.set_public_cache(cache.is_public);

    let mut narinfo = object.to_nar_info(&nar)?;

    if narinfo.signature().is_none() {
        let keypair = cache.keypair()?;
        narinfo.sign(&keypair);
    }

    let mut response = narinfo.into_response();
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        cache_control(cache.is_public, 300, false),
    );

    Ok(response)
}

/// Returns whether an object's `last_accessed_at` timestamp is due for a bump.
///
/// A bump is needed when the object has never been accessed, or when the
/// last recorded access is older than [`LAST_ACCESSED_DEBOUNCE`].
fn should_bump_last_accessed(last_accessed_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    match last_accessed_at {
        None => true,
        Some(last_accessed_at) => now - last_accessed_at >= LAST_ACCESSED_DEBOUNCE,
    }
}

/// Gets a NAR.
///
/// - GET `:cache/nar/{storePathHash}.nar`
///
/// Here we use the store path hash not the NAR hash or file hash
/// for better logging. In reality, the files are deduplicated by
/// content-addressing.
#[instrument(skip_all, fields(cache_name, path))]
async fn get_nar(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path((cache_name, path)): Path<(CacheName, String)>,
) -> ServerResult<Response> {
    let components: Vec<&str> = path.splitn(2, '.').collect();

    if components.len() != 2 {
        return Err(ErrorKind::NotFound.into());
    }

    if components[1] != "nar" {
        return Err(ErrorKind::NotFound.into());
    }

    let store_path_hash = StorePathHash::new(components[0].to_string())?;

    tracing::debug!(
        "Received request for {}.nar in {:?}",
        store_path_hash.as_str(),
        cache_name
    );

    let database = state.database().await?;

    let (object, cache, _nar, chunks) = database
        .find_object_and_chunks_by_store_path_hash(&cache_name, &store_path_hash, true)
        .await?;

    let permission = req_state
        .auth
        .get_permission_for_cache(&cache_name, cache.is_public);
    permission.require_pull()?;

    req_state.set_public_cache(cache.is_public);

    // TODO: Fully kill chunk recovery
    if chunks.iter().any(Option::is_none) {
        // at least one of the chunks is missing :(
        return Err(ErrorKind::IncompleteNar.into());
    }

    if should_bump_last_accessed(object.last_accessed_at, Utc::now()) {
        // The timestamp only feeds retention-based GC (hours-to-days
        // granularity), so we don't need to await this write or fail the
        // request if it errors out. Spawning it off the request path avoids
        // putting a database round-trip in front of every NAR download,
        // which would otherwise serialize concurrent downloads through
        // SQLite's single writer.
        let database = database.clone();
        let object_id = object.id;
        tokio::spawn(async move {
            if let Err(e) = database.bump_object_last_accessed(object_id).await {
                tracing::warn!(%e, "Failed to bump last_accessed_at for object {}", object_id);
            }
        });
    }

    if chunks.len() == 1 {
        // single chunk
        let chunk = chunks[0].as_ref().unwrap();
        let remote_file = &chunk.remote_file.0;
        let storage = state.storage().await?;
        match storage.download_file_db(remote_file, false).await? {
            Download::Url(url) => {
                let mut response = Redirect::temporary(&url).into_response();
                response.headers_mut().insert(
                    http::header::CACHE_CONTROL,
                    cache_control(cache.is_public, 60, false),
                );

                Ok(response)
            }
            Download::AsyncRead(stream) => {
                let stream = ReaderStream::with_capacity(stream, STREAM_BUFFER_SIZE).map_err(|e| {
                    tracing::error!(%e, "Stream error");
                    e
                });
                let body = Body::from_stream(stream);

                Ok((
                    [
                        (
                            http::header::CONTENT_TYPE,
                            http::HeaderValue::from_static(mime::NAR),
                        ),
                        (
                            http::header::CACHE_CONTROL,
                            cache_control(cache.is_public, 31_536_000, true),
                        ),
                    ],
                    body,
                )
                    .into_response())
            }
        }
    } else {
        // reassemble NAR
        fn io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> IoError {
            IoError::other(e)
        }

        let streamer = |chunk: ChunkModel, storage: Arc<StorageBackendImpl>| async move {
            match storage
                .download_file_db(&chunk.remote_file.0, true)
                .await
                .map_err(io_error)?
            {
                Download::Url(_) => Err(IoError::other("URLs not supported for NAR reassembly")),
                Download::AsyncRead(stream) => {
                    let stream: BoxStream<_> =
                        Box::pin(ReaderStream::with_capacity(stream, STREAM_BUFFER_SIZE));
                    Ok(stream)
                }
            }
        };

        let chunks: VecDeque<_> = chunks.into_iter().map(Option::unwrap).collect();
        let storage = state.storage().await?.clone();

        let num_prefetch = state.config.nar_reassembly_prefetch;
        let merged = merge_chunks(chunks, streamer, storage, num_prefetch).map_err(|e| {
            tracing::error!(%e, "Stream error");
            e
        });
        let body = Body::from_stream(merged);

        Ok((
            [
                (
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static(mime::NAR),
                ),
                (
                    http::header::CACHE_CONTROL,
                    cache_control(cache.is_public, 31_536_000, true),
                ),
            ],
            body,
        )
            .into_response())
    }
}

pub fn get_router() -> Router {
    Router::new()
        .route("/{cache}/nix-cache-info", get(get_nix_cache_info))
        .route("/{cache}/{path}", get(get_store_path_info))
        .route("/{cache}/nar/{path}", get(get_nar))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_bump_last_accessed_none_is_true() {
        let now = Utc::now();
        assert!(should_bump_last_accessed(None, now));
    }

    #[test]
    fn should_bump_last_accessed_stale_is_true() {
        let now = Utc::now();
        let two_hours_ago = now - chrono::Duration::hours(2);
        assert!(should_bump_last_accessed(Some(two_hours_ago), now));
    }

    #[test]
    fn should_bump_last_accessed_recent_is_false() {
        let now = Utc::now();
        let one_minute_ago = now - chrono::Duration::minutes(1);
        assert!(!should_bump_last_accessed(Some(one_minute_ago), now));
    }

    #[test]
    fn cache_control_public_immutable() {
        let value = cache_control(true, 31_536_000, true);
        assert_eq!(value, "public, max-age=31536000, immutable");
    }

    #[test]
    fn cache_control_private_not_immutable() {
        let value = cache_control(false, 300, false);
        assert_eq!(value, "private, max-age=300");
    }
}
