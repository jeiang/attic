//! Store path uploader.
//!
//! There are two APIs: `Pusher` and `PushSession`.
//!
//! A `Pusher` simply dispatches `ValidPathInfo`s for workers to push. Use this
//! when you know all store paths to push beforehand. The push plan (closure, missing
//! paths, all path metadata) should be computed prior to pushing.
//!
//! A `PushSession`, on the other hand, accepts a stream of `StorePath`s and
//! takes care of retrieving the closure and path metadata. It automatically
//! batches expensive operations (closure computation, querying missing paths).
//! Use this when the list of store paths is streamed from some external
//! source (e.g., FS watcher, Unix Domain Socket) and a push plan cannot be
//! created statically.
//!
//! TODO: Refactor out progress reporting and support a simple output style without progress bars

use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use async_channel as channel;
use bytes::{Buf, Bytes, BytesMut};
use futures::future::join_all;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{Mutex, mpsc};
use tokio::task::{JoinHandle, spawn};
use tokio::time;

use crate::api::{ApiClient, UploadAttempt};
use attic::api::v1::cache_config::{CacheConfig, ChunkingParameters};
use attic::api::v1::upload_path::{
    ChunkManifestEntry, UploadPathNarInfo, UploadPathResult, UploadPathResultKind,
    build_chunk_manifest,
};
use attic::cache::CacheName;
use attic::chunking::chunk_stream;
use attic::error::{AtticError, AtticResult};
use attic::hash::Hash;
use attic::io::read_chunk_async;
use attic::nix_store::{NixStore, StorePath, StorePathHash, ValidPathInfo};

type JobSender = channel::Sender<ValidPathInfo>;
type JobReceiver = channel::Receiver<ValidPathInfo>;

/// Configuration for pushing store paths.
#[derive(Clone, Copy, Debug)]
pub struct PushConfig {
    /// The number of workers to spawn.
    pub num_workers: usize,

    /// Whether to always include the upload info in the PUT payload.
    pub force_preamble: bool,
}

/// Configuration for a push session.
#[derive(Clone, Copy, Debug)]
pub struct PushSessionConfig {
    /// Push the specified paths only and do not compute closures.
    pub no_closure: bool,

    /// Ignore the upstream cache filter.
    pub ignore_upstream_cache_filter: bool,
}

/// A handle to push store paths to a cache.
///
/// The caller is responsible for computing closures and
/// checking for paths that already exist on the remote
/// cache.
pub struct Pusher {
    api: ApiClient,
    store: Arc<NixStore>,
    cache: CacheName,
    cache_config: Arc<CacheConfig>,
    workers: Vec<JoinHandle<HashMap<StorePath, Result<()>>>>,
    sender: JobSender,
}

/// A wrapper over a `Pusher` that accepts a stream of `StorePath`s.
///
/// Unlike a `Pusher`, a `PushSession` takes a stream of `StorePath`s
/// instead of `ValidPathInfo`s, taking care of retrieving the closure
/// and path metadata.
///
/// This is useful when the list of store paths is streamed from some
/// external source (e.g., FS watcher, Unix Domain Socket) and a push
/// plan cannot be computed statically.
///
/// ## Batching
///
/// Many store paths can be built in a short period of time, with each
/// having a big closure. It can be very inefficient if we were to compute
/// closure and query for missing paths for each individual path. This is
/// especially true if we have a lot of remote builders (e.g., `attic watch-store`
/// running alongside a beefy Hydra instance).
///
/// `PushSession` batches operations in order to minimize the number of
/// closure computations and API calls. It also remembers which paths already
/// exist on the remote cache. By default, it submits a batch if it's been 2
/// seconds since the last path is queued or it's been 10 seconds in total.
pub struct PushSession {
    /// Sender to the batching future.
    sender: channel::Sender<SessionQueueCommand>,

    /// Receiver of results.
    result_receiver: mpsc::Receiver<Result<HashMap<StorePath, Result<()>>>>,
}

enum SessionQueueCommand {
    Paths(Vec<StorePath>),
    Flush,
    Terminate,
}

enum SessionQueuePoll {
    Paths(Vec<StorePath>),
    Flush,
    Terminate,
    Closed,
    TimedOut,
}

#[derive(Debug)]
pub struct PushPlan {
    /// Store paths to push.
    pub store_path_map: HashMap<StorePathHash, ValidPathInfo>,

    /// The number of paths in the original full closure.
    pub num_all_paths: usize,

    /// Number of paths that have been filtered out because they are already cached.
    pub num_already_cached: usize,

    /// Number of paths that have been filtered out because they are signed by an upstream cache.
    pub num_upstream: usize,
}

/// Wrapper to update a progress bar as a NAR is streamed.
struct NarStreamProgress<S> {
    stream: S,
    bar: ProgressBar,
}

impl Pusher {
    pub fn new(
        store: Arc<NixStore>,
        api: ApiClient,
        cache: CacheName,
        cache_config: CacheConfig,
        mp: MultiProgress,
        config: PushConfig,
    ) -> Self {
        let cache_config = Arc::new(cache_config);
        let (sender, receiver) = channel::unbounded();
        let mut workers = Vec::new();

        for _ in 0..config.num_workers {
            workers.push(spawn(Self::worker(
                receiver.clone(),
                store.clone(),
                api.clone(),
                cache.clone(),
                cache_config.clone(),
                mp.clone(),
                config,
            )));
        }

        Self {
            api,
            store,
            cache,
            cache_config,
            workers,
            sender,
        }
    }

    /// Queues a store path to be pushed.
    pub async fn queue(&self, path_info: ValidPathInfo) -> Result<()> {
        self.sender.send(path_info).await.map_err(|e| anyhow!(e))
    }

    /// Waits for all workers to terminate, returning all results.
    ///
    /// TODO: Stream the results with another channel
    pub async fn wait(self) -> HashMap<StorePath, Result<()>> {
        drop(self.sender);

        join_all(self.workers)
            .await
            .into_iter()
            .map(|joinresult| joinresult.unwrap())
            .fold(HashMap::new(), |mut acc, results| {
                acc.extend(results);
                acc
            })
    }

    /// Creates a push plan.
    pub async fn plan(
        &self,
        roots: Vec<StorePath>,
        no_closure: bool,
        ignore_upstream_filter: bool,
    ) -> Result<PushPlan> {
        PushPlan::plan(
            self.store.clone(),
            &self.api,
            &self.cache,
            &self.cache_config,
            roots,
            no_closure,
            ignore_upstream_filter,
        )
        .await
    }

    /// Converts the pusher into a `PushSession`.
    ///
    /// This is useful when the list of store paths is streamed from some
    /// external source (e.g., FS watcher, Unix Domain Socket) and a push
    /// plan cannot be computed statically.
    pub fn into_push_session(self, config: PushSessionConfig) -> PushSession {
        PushSession::with_pusher(self, config)
    }

    async fn worker(
        receiver: JobReceiver,
        store: Arc<NixStore>,
        api: ApiClient,
        cache: CacheName,
        cache_config: Arc<CacheConfig>,
        mp: MultiProgress,
        config: PushConfig,
    ) -> HashMap<StorePath, Result<()>> {
        let mut results = HashMap::new();

        loop {
            let path_info = match receiver.recv().await {
                Ok(path_info) => path_info,
                Err(_) => {
                    // channel is closed - we are done
                    break;
                }
            };

            let store_path = path_info.path.clone();

            let r = upload_path(
                path_info,
                store.clone(),
                api.clone(),
                &cache,
                &cache_config,
                mp.clone(),
                config.force_preamble,
            )
            .await;

            results.insert(store_path, r);
        }

        results
    }
}

impl PushSession {
    pub fn with_pusher(pusher: Pusher, config: PushSessionConfig) -> Self {
        let (sender, receiver) = channel::unbounded();
        let (result_sender, result_receiver) = mpsc::channel(1);

        let known_paths_mutex = Arc::new(Mutex::new(HashSet::new()));

        spawn(async move {
            if let Err(e) = Self::worker(
                pusher,
                config,
                known_paths_mutex.clone(),
                receiver.clone(),
                result_sender.clone(),
            )
            .await
            {
                let _ = result_sender.send(Err(e)).await;
            }
        });

        Self {
            sender,
            result_receiver,
        }
    }

    async fn worker(
        pusher: Pusher,
        config: PushSessionConfig,
        known_paths_mutex: Arc<Mutex<HashSet<StorePathHash>>>,
        receiver: channel::Receiver<SessionQueueCommand>,
        result_sender: mpsc::Sender<Result<HashMap<StorePath, Result<()>>>>,
    ) -> Result<()> {
        let mut roots = HashSet::new();

        loop {
            // Get outstanding paths in queue
            let done = tokio::select! {
                // 2 seconds since last queued path
                done = async {
                    loop {
                        let poll = tokio::select! {
                            r = receiver.recv() => match r {
                                Ok(SessionQueueCommand::Paths(paths)) => SessionQueuePoll::Paths(paths),
                                Ok(SessionQueueCommand::Flush) => SessionQueuePoll::Flush,
                                Ok(SessionQueueCommand::Terminate) => SessionQueuePoll::Terminate,
                                _ => SessionQueuePoll::Closed,
                            },
                            _ = time::sleep(Duration::from_secs(2)) => SessionQueuePoll::TimedOut,
                        };

                        match poll {
                            SessionQueuePoll::Paths(store_paths) => {
                                roots.extend(store_paths.into_iter());
                            }
                            SessionQueuePoll::Closed | SessionQueuePoll::Terminate => {
                                break true;
                            }
                            SessionQueuePoll::Flush | SessionQueuePoll::TimedOut => {
                                break false;
                            }
                        }
                    }
                } => done,

                // 10 seconds
                _ = time::sleep(Duration::from_secs(10)) => {
                    false
                },
            };

            // Compute push plan
            let roots_vec: Vec<StorePath> = {
                let known_paths = known_paths_mutex.lock().await;
                roots
                    .drain()
                    .filter(|root| !known_paths.contains(&root.to_hash()))
                    .collect()
            };

            let mut plan = pusher
                .plan(
                    roots_vec,
                    config.no_closure,
                    config.ignore_upstream_cache_filter,
                )
                .await?;

            let mut known_paths = known_paths_mutex.lock().await;
            plan.store_path_map
                .retain(|sph, _| !known_paths.contains(sph));

            // Push everything
            for (store_path_hash, path_info) in plan.store_path_map.into_iter() {
                pusher.queue(path_info).await?;
                known_paths.insert(store_path_hash);
            }

            drop(known_paths);

            if done {
                let result = pusher.wait().await;
                result_sender.send(Ok(result)).await?;
                return Ok(());
            }
        }
    }

    /// Waits for all workers to terminate, returning all results.
    pub async fn wait(mut self) -> Result<HashMap<StorePath, Result<()>>> {
        self.flush()?;

        // The worker might have died
        let _ = self.sender.send(SessionQueueCommand::Terminate).await;

        self.result_receiver
            .recv()
            .await
            .expect("Nothing in result channel")
    }

    /// Queues multiple store paths to be pushed.
    pub fn queue_many(&self, store_paths: Vec<StorePath>) -> Result<()> {
        self.sender
            .send_blocking(SessionQueueCommand::Paths(store_paths))
            .map_err(|e| anyhow!(e))
    }

    /// Flushes the worker queue.
    pub fn flush(&self) -> Result<()> {
        self.sender
            .send_blocking(SessionQueueCommand::Flush)
            .map_err(|e| anyhow!(e))
    }
}

impl PushPlan {
    /// Creates a plan.
    async fn plan(
        store: Arc<NixStore>,
        api: &ApiClient,
        cache: &CacheName,
        cache_config: &CacheConfig,
        roots: Vec<StorePath>,
        no_closure: bool,
        ignore_upstream_filter: bool,
    ) -> Result<Self> {
        // Compute closure
        let closure = if no_closure {
            roots
        } else {
            store
                .compute_fs_closure_multi(roots, false, false, false)
                .await?
        };

        let mut store_path_map: HashMap<StorePathHash, ValidPathInfo> = {
            let futures = closure
                .iter()
                .map(|path| {
                    let store = store.clone();
                    let path = path.clone();
                    let path_hash = path.to_hash();

                    async move {
                        let path_info = store.query_path_info(path).await?;
                        Ok((path_hash, path_info))
                    }
                })
                .collect::<Vec<_>>();

            join_all(futures).await.into_iter().collect::<Result<_>>()?
        };

        let num_all_paths = store_path_map.len();
        if store_path_map.is_empty() {
            return Ok(Self {
                store_path_map,
                num_all_paths,
                num_already_cached: 0,
                num_upstream: 0,
            });
        }

        if !ignore_upstream_filter {
            // Filter out paths signed by upstream caches
            let upstream_cache_key_names = cache_config
                .upstream_cache_key_names
                .as_ref()
                .map_or([].as_slice(), |v| v.as_slice());
            store_path_map.retain(|_, pi| {
                for sig in &pi.sigs {
                    if let Some((name, _)) = sig.split_once(':')
                        && upstream_cache_key_names.iter().any(|u| name == u)
                    {
                        return false;
                    }
                }

                true
            });
        }

        let num_filtered_paths = store_path_map.len();
        if store_path_map.is_empty() {
            return Ok(Self {
                store_path_map,
                num_all_paths,
                num_already_cached: 0,
                num_upstream: num_all_paths - num_filtered_paths,
            });
        }

        // Query missing paths
        let missing_path_hashes: HashSet<StorePathHash> = {
            let store_path_hashes = store_path_map.keys().map(|sph| sph.to_owned()).collect();
            let res = api.get_missing_paths(cache, store_path_hashes).await?;
            res.missing_paths.into_iter().collect()
        };
        store_path_map.retain(|sph, _| missing_path_hashes.contains(sph));
        let num_missing_paths = store_path_map.len();

        Ok(Self {
            store_path_map,
            num_all_paths,
            num_already_cached: num_filtered_paths - num_missing_paths,
            num_upstream: num_all_paths - num_filtered_paths,
        })
    }
}

/// Uploads a single path to a cache.
pub async fn upload_path(
    path_info: ValidPathInfo,
    store: Arc<NixStore>,
    api: ApiClient,
    cache: &CacheName,
    cache_config: &CacheConfig,
    mp: MultiProgress,
    force_preamble: bool,
) -> Result<()> {
    let path = &path_info.path;
    let nar_size = path_info.nar_size as usize;

    let full_path = store
        .get_full_path(path)
        .to_str()
        .ok_or_else(|| anyhow!("Path contains non-UTF-8"))?
        .to_string();

    let references = path_info
        .references
        .iter()
        .map(|pb| {
            pb.to_str()
                .ok_or_else(|| anyhow!("Reference contains non-UTF-8"))
                .map(|s| s.to_owned())
        })
        .collect::<Result<Vec<String>, anyhow::Error>>()?;

    let store_path_hash = path.to_hash();
    let sigs = path_info.sigs.clone();
    let ca = path_info.ca.clone();
    let nar_hash = path_info.nar_hash.clone();

    // Builds the upload info for either a negotiated (with a chunk
    // manifest) or a plain (full-upload) attempt. Cloning the small bits of
    // metadata here is cheap and lets us prepare both request variants up
    // front, since only one of them will ever have its body streamed.
    let build_upload_info = |chunk_manifest: Option<Vec<ChunkManifestEntry>>| UploadPathNarInfo {
        cache: cache.to_owned(),
        store_path_hash: store_path_hash.clone(),
        store_path: full_path.clone(),
        references: references.clone(),
        system: None,  // TODO
        deriver: None, // TODO
        sigs: sigs.clone(),
        ca: ca.clone(),
        nar_hash: nar_hash.clone(),
        nar_size,
        chunk_manifest,
    };

    let template = format!(
        "{{spinner}} {: <20.20} {{bar:40.green/blue}} {{human_bytes:10}} ({{average_speed}})",
        path.name(),
    );
    let style = ProgressStyle::with_template(&template)
        .unwrap()
        .tick_chars("🕛🕐🕑🕒🕓🕔🕕🕖🕗🕘🕙🕚✅")
        .progress_chars("██ ")
        .with_key("human_bytes", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{}", HumanBytes(state.pos())).unwrap();
        })
        // Adapted from
        // <https://github.com/console-rs/indicatif/issues/394#issuecomment-1309971049>
        .with_key(
            "average_speed",
            |state: &ProgressState, w: &mut dyn Write| match (state.pos(), state.elapsed()) {
                (pos, elapsed) if elapsed > Duration::ZERO => {
                    write!(w, "{}", average_speed(pos, elapsed)).unwrap();
                }
                _ => write!(w, "-").unwrap(),
            },
        );
    let bar = mp.add(ProgressBar::new(path_info.nar_size));
    bar.set_style(style);

    // Negotiate a chunk manifest iff the server advertises chunking
    // parameters and this NAR is large enough to trigger chunking
    // server-side. A threshold of 0 disables chunking server-side (and the
    // server won't advertise `chunking` in that case), but we keep this
    // check client-side too for extra safety.
    let negotiated = match cache_config.chunking.as_ref() {
        Some(chunking) if nar_size >= chunking.nar_size_threshold => {
            match negotiate_chunk_manifest(&store, path, chunking, &api, cache).await {
                Ok(negotiated) => negotiated,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        "Chunk-manifest negotiation failed; falling back to full upload"
                    );
                    None
                }
            }
        }
        _ => None,
    };

    // The manifest can be too large for a header, so negotiated uploads
    // always use the preamble.
    let negotiated_request = negotiated
        .as_ref()
        .map(|n: &NegotiatedManifest| {
            api.prepare_upload_path(
                build_upload_info(Some(n.manifest.clone())),
                true,
                n.total_inline_bytes,
            )
        })
        .transpose()?;
    let full_request =
        api.prepare_upload_path(build_upload_info(None), force_preamble, nar_size)?;

    let start = Instant::now();

    // Attempt 1 is the negotiated upload, if any; any subsequent attempt
    // (including the only attempt when there's no negotiated manifest) is a
    // full, non-negotiated upload. A negotiated attempt that fails for any
    // reason (a race with GC, a manifest too large for the server, etc.)
    // falls back to the full-upload path immediately, without consuming one
    // of `upload_path_with_retry`'s retry attempts - a server that doesn't
    // understand chunk manifests never receives one in the first place
    // (negotiation is capability-gated on `cache_config.chunking`), so this
    // fallback exists purely for negotiated-specific failures.
    let mut negotiated_outcome = None;
    if let Some(n) = &negotiated {
        bar.set_position(0);
        bar.set_length(n.total_inline_bytes as u64);
        tracing::debug!("Starting negotiated (chunk-manifest) path upload attempt");

        let raw_stream = store.nar_from_path(path.to_owned());
        let adapted = negotiated_send_stream(raw_stream, n.manifest.clone());
        let nar_stream = NarStreamProgress::new(adapted, bar.clone()).map_ok(Bytes::from);
        let request = negotiated_request
            .as_ref()
            .expect("negotiated_request is set whenever negotiated is Some");

        match api.upload_path_attempt(request, nar_stream).await {
            Ok(value) => negotiated_outcome = Some(Ok(UploadAttempt { value, attempts: 1 })),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Negotiated upload attempt failed; falling back to full upload"
                );
            }
        }
    }

    let upload_result = match negotiated_outcome {
        Some(result) => result,
        None => {
            bar.set_position(0);
            bar.set_length(path_info.nar_size);
            api.upload_path_with_retry(|attempt| {
                bar.set_position(0);
                tracing::debug!(attempt, "Starting path upload attempt");
                let nar_stream =
                    NarStreamProgress::new(store.nar_from_path(path.to_owned()), bar.clone())
                        .map_ok(Bytes::from);
                api.upload_path_attempt(&full_request, nar_stream)
            })
            .await
        }
    };

    match upload_result {
        Ok(upload) => {
            let r = upload.value.unwrap_or(UploadPathResult {
                kind: UploadPathResultKind::Uploaded,
                file_size: None,
                frac_deduplicated: None,
            });

            let info_string: String = match r.kind {
                UploadPathResultKind::Deduplicated => "deduplicated".to_string(),
                _ => {
                    let elapsed = start.elapsed();
                    let seconds = elapsed.as_secs_f64();
                    let speed = (path_info.nar_size as f64 / seconds) as u64;

                    let mut s = format!("{}/s", HumanBytes(speed));

                    if let Some(frac_deduplicated) = r.frac_deduplicated
                        && frac_deduplicated > 0.01f64
                    {
                        s += &format!(", {:.1}% deduplicated", frac_deduplicated * 100.0);
                    }

                    s
                }
            };

            mp.suspend(|| {
                eprintln!(
                    "✅ {} ({})",
                    path.as_os_str().to_string_lossy(),
                    info_string
                );
            });
            bar.finish_and_clear();

            Ok(())
        }
        Err(e) => {
            mp.suspend(|| {
                eprintln!("❌ {}: {}", path.as_os_str().to_string_lossy(), e);
            });
            bar.finish_and_clear();
            Err(e.into())
        }
    }
}

/// A chunk manifest negotiated with the server, ready to send.
struct NegotiatedManifest {
    /// The manifest, in NAR stream order.
    manifest: Vec<ChunkManifestEntry>,

    /// The total size, in bytes, of the manifest's `inline` entries.
    ///
    /// This is the size of the request body a negotiated upload will
    /// actually send, used both for `Content-Length` and to size the
    /// progress bar.
    total_inline_bytes: usize,
}

/// Attempts client-side chunk-manifest negotiation for a NAR upload.
///
/// This runs the same content-defined chunking algorithm the server uses
/// (with the server-advertised parameters) over the NAR in a first "hash
/// pass" that discards each chunk's bytes as soon as it's hashed, then asks
/// the server (via `get-missing-chunks`) which of those chunks it already
/// has.
///
/// Returns `Ok(None)` when negotiating would provide no benefit: the NAR
/// produced no chunks, or every unique chunk is missing server-side (in
/// which case a manifest would end up with everything inline, which is
/// pure overhead compared to a plain full upload). The caller should treat
/// this the same as "no manifest", not as a failure.
///
/// Returns `Err` if negotiation itself failed (e.g. the `get-missing-chunks`
/// call errored). The caller should fall back to a full upload without
/// treating this as a consumed retry attempt.
async fn negotiate_chunk_manifest(
    store: &Arc<NixStore>,
    path: &StorePath,
    chunking: &ChunkingParameters,
    api: &ApiClient,
    cache: &CacheName,
) -> Result<Option<NegotiatedManifest>> {
    let raw_stream = store.nar_from_path(path.to_owned());
    let reader = StreamAsyncReader::new(raw_stream);
    let mut chunks = chunk_stream(
        reader,
        chunking.min_size,
        chunking.avg_size,
        chunking.max_size,
    );

    let mut ordered_chunks: Vec<(Hash, usize)> = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        let hash = Hash::sha256_from_bytes(&chunk);
        ordered_chunks.push((hash, chunk.len()));
    }

    if ordered_chunks.is_empty() {
        return Ok(None);
    }

    let mut seen = HashSet::new();
    let mut unique_hashes = Vec::new();
    for (hash, _) in &ordered_chunks {
        if seen.insert(hash.clone()) {
            unique_hashes.push(hash.clone());
        }
    }

    let response = api.get_missing_chunks(cache, unique_hashes.clone()).await?;
    let missing: HashSet<Hash> = response.missing_chunks.into_iter().collect();

    if unique_hashes.iter().all(|hash| missing.contains(hash)) {
        // Every unique chunk is missing server-side: a manifest would
        // inline everything, which is pure overhead over a full upload.
        return Ok(None);
    }

    let manifest = build_chunk_manifest(&ordered_chunks, &missing);
    let total_inline_bytes = manifest
        .iter()
        .filter(|entry| entry.inline)
        .map(|entry| entry.size)
        .sum();

    Ok(Some(NegotiatedManifest {
        manifest,
        total_inline_bytes,
    }))
}

/// Re-streams a NAR, re-sliced according to a chunk manifest's recorded
/// entry sizes, yielding only the bytes of `inline` entries in order.
///
/// The manifest's chunk boundaries were computed by `chunk_stream` in
/// `negotiate_chunk_manifest`'s hash pass and generally don't line up with
/// the arbitrary-sized reads the underlying byte stream happens to
/// produce, so this re-slices by byte count (via `read_chunk_async`)
/// rather than reusing the stream's own chunk boundaries. NAR serialization
/// is deterministic, so re-running `NixStore::nar_from_path` on the same
/// path reproduces the exact same bytes the hash pass saw.
fn negotiated_send_stream<S>(
    raw_stream: S,
    manifest: Vec<ChunkManifestEntry>,
) -> Pin<Box<dyn Stream<Item = AtticResult<Vec<u8>>> + Send + Sync>>
where
    S: Stream<Item = AtticResult<Vec<u8>>> + Unpin + Send + Sync + 'static,
{
    let reader = StreamAsyncReader::new(raw_stream);

    // Boxed and pinned because the state machine `stream::unfold` builds
    // around our `async move` closure is not itself `Unpin`, but
    // `NarStreamProgress`/`upload_path_attempt` need an `Unpin + Sync`
    // stream; `Pin<Box<_>>` is unconditionally `Unpin` regardless of the
    // pointee, and `Sync` is requested explicitly on the trait object below
    // (matching `upload_path_attempt`'s bound).
    Box::pin(stream::unfold(
        (reader, manifest.into_iter()),
        |(mut reader, mut entries)| async move {
            loop {
                let entry = entries.next()?;

                let buf = BytesMut::with_capacity(entry.size);
                let bytes = match read_chunk_async(&mut reader, buf).await {
                    Ok(bytes) => bytes,
                    Err(e) => return Some((Err(AtticError::from(e)), (reader, entries))),
                };

                if bytes.len() != entry.size {
                    let err = std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!(
                            "Re-streamed NAR ended early: expected {} more bytes for a chunk \
                             manifest entry but only got {}",
                            entry.size,
                            bytes.len()
                        ),
                    );
                    return Some((Err(AtticError::from(err)), (reader, entries)));
                }

                if entry.inline {
                    return Some((Ok(bytes.to_vec()), (reader, entries)));
                }

                // A reference: bytes have already been consumed from the
                // stream to keep it in sync; discard them and move on to
                // the next manifest entry.
            }
        },
    ))
}

/// Adapts a `Stream<Item = AtticResult<Vec<u8>>>` (as produced by
/// `NixStore::nar_from_path`) into a `tokio::io::AsyncRead`, so it can be
/// fed into `attic::chunking::chunk_stream` (the hash pass) and
/// `attic::io::read_chunk_async` (the send pass).
struct StreamAsyncReader<S> {
    stream: S,
    leftover: Bytes,
}

impl<S> StreamAsyncReader<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            leftover: Bytes::new(),
        }
    }
}

impl<S> AsyncRead for StreamAsyncReader<S>
where
    S: Stream<Item = AtticResult<Vec<u8>>> + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        loop {
            if !this.leftover.is_empty() {
                let n = this.leftover.len().min(buf.remaining());
                buf.put_slice(&this.leftover[..n]);
                this.leftover.advance(n);
                return Poll::Ready(Ok(()));
            }

            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(data))) => {
                    if data.is_empty() {
                        continue;
                    }
                    this.leftover = Bytes::from(data);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(std::io::Error::other(e)));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: Stream<Item = AtticResult<Vec<u8>>>> NarStreamProgress<S> {
    fn new(stream: S, bar: ProgressBar) -> Self {
        Self { stream, bar }
    }
}

impl<S: Stream<Item = AtticResult<Vec<u8>>> + Unpin> Stream for NarStreamProgress<S> {
    type Item = AtticResult<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).as_mut().poll_next(cx) {
            Poll::Ready(Some(data)) => {
                if let Ok(data) = &data {
                    self.bar.inc(data.len() as u64);
                }

                Poll::Ready(Some(data))
            }
            other => other,
        }
    }
}

// Just the average, no fancy sliding windows that cause wild fluctuations
// <https://github.com/console-rs/indicatif/issues/394>
fn average_speed(bytes: u64, duration: Duration) -> String {
    let speed = bytes as f64 * 1000_f64 / duration.as_millis() as f64;
    format!("{}/s", HumanBytes(speed as u64))
}
