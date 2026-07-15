# Attic Performance Optimizations

Suggestions for improving throughput and latency, ordered from highest to
lowest expected impact. This complements `OPTIMIZATION_PLAN.md`, which focused
on memory/OOM hardening (its steps — global upload semaphores, bounded S3
multipart concurrency, jemalloc, SQLite `mmap_size`, pool settings, GC
batching — have already landed). Everything below targets speed.

The two hot paths are:

- **Download** (`GET /:cache/{hash}.narinfo`, `GET /:cache/nar/{hash}.nar`) —
  usually dominates: many machines substitute from the cache.
- **Upload** (`PUT /_api/v1/upload-path`) — CI pushing closures.

---

## 1. Add HTTP caching headers to narinfo/NAR responses

**Files:** `server/src/api/binary_cache.rs`, `server/src/narinfo/mod.rs`

Responses currently carry no `Cache-Control`/`ETag` headers at all. NAR
content is content-addressed and therefore immutable — it is the ideal case
for `Cache-Control: public, max-age=31536000, immutable` (for public caches).
Signed `.narinfo` responses can get a shorter TTL (e.g. minutes) since
signatures and cache visibility can change.

This is the single biggest lever available: it lets Nix clients, corporate
proxies, and most importantly a CDN (CloudFront/Fastly/Cloudflare in front of
`atticd`) absorb the read traffic entirely. The server then only handles cache
misses, and every downstream request gets edge latency instead of a
DB-query-plus-S3-roundtrip. Care points: only emit `public` for public caches;
`404` responses for private caches must stay uncacheable.

**Impact: very high** (read path, enables horizontal offload) · **Effort: low** · **Risk: low**

## 2. Take the `last_accessed` bump off the download critical path

**File:** `server/src/api/binary_cache.rs:212`

`get_nar` executes `bump_object_last_accessed` — a synchronous `UPDATE` — and
*awaits it* before the first byte of the NAR is streamed. Every download pays
a DB write round-trip of latency, and on SQLite all concurrent downloads
serialize through the single writer; a busy read-mostly cache becomes
write-bound for no user-visible benefit (the timestamp only feeds LRU GC,
which runs at coarse intervals).

Two complementary fixes:

1. Fire-and-forget: `tokio::spawn` the update (same pattern the
   `NarGuard`/`ChunkGuard` drop handlers already use in
   `server/src/database/mod.rs`).
2. Debounce: skip the write when `last_accessed_at` is already within, say,
   1 hour — the row is already in hand from the lookup join, so this costs
   nothing and eliminates ~all writes for hot paths.

**Impact: high** (removes a write from every download; biggest on SQLite) · **Effort: low** · **Risk: low**

## 3. Chunk-level dedup negotiation in the push protocol

**Files:** `attic/src/api/v1/` (new endpoint types), `server/src/api/v1/`,
`client/src/push.rs`

Deduplication currently happens entirely server-side: the client always
uploads the **full uncompressed NAR**, the server chunks it and discards
chunks it already has. A push of a rebuilt closure where 95% of chunks are
already stored still transfers 100% of the bytes.

Mirror the existing `get-missing-paths` flow at the chunk level: the client
runs the same FastCDC chunking locally (`attic/src/chunking/` already builds
for the client and WASM), sends the chunk-hash list, receives the missing
subset, and uploads only those chunks. This is how restic/borg/casync-style
tools get their incremental-upload wins. It is a protocol addition (needs
versioning/fallback), but for CI workloads that push slightly-changed closures
repeatedly it routinely cuts transfer volume by an order of magnitude, and
upload wall-time with it.

**Impact: high** (upload bandwidth/time for incremental pushes) · **Effort: high** · **Risk: medium** (protocol change, needs backward compatibility)

## 4. Batch per-chunk database round-trips during chunked uploads

**Files:** `server/src/api/v1/upload_path.rs` (`upload_path_new_chunked`,
`upload_chunk`)

Each chunk of a chunked upload performs up to four sequential statements:
`find_and_lock_chunk` (UPDATE…RETURNING), pending-chunk INSERT, final
UPDATE-to-Valid, and a `chunkref` INSERT. With the default 256 KiB average
chunk size, a 1 GiB NAR issues ~16,000 statements. On Postgres each is a
network round-trip; on SQLite each write takes the global write lock.

Options, in increasing order of ambition:

- Buffer the `chunkref` rows and `insert_many` them in the final transaction
  (they are only meaningful once the NAR flips to `Valid` anyway). Cuts one
  statement per chunk with no semantic change.
- Batch dedup lookups: collect N chunk hashes and probe them with a single
  `IN` query, falling back to individual lock-acquisition only on hits.
- Combine the pending-INSERT/final-UPDATE pair for chunks that finish quickly.

**Impact: medium-high** (upload throughput, DB load; biggest on Postgres over a network) · **Effort: medium** · **Risk: medium** (touches upload correctness/cleanup paths)

## 5. Eliminate the redundant SHA-256 pass and keep hashing off the reactor

**Files:** `server/src/api/v1/upload_path.rs` (`ChunkData::hash`),
`server/src/compression.rs`

Every chunk's data is hashed **twice**: once in `ChunkData::hash()` (to probe
for dedup) and again inside `CompressionStream`'s NAR-hash `HashReader` (to
"verify" it). For the `ChunkData::Bytes` case both passes run over the same
in-memory buffer the server itself produced — the second pass can never
disagree with the first, so one of them is pure waste. On top of that there's
a third whole-NAR-level hash pass in `upload_path_new_chunked`'s outer
`HashReader`, which *is* needed (it validates the client's claim). Dropping
the redundant per-chunk pass saves roughly a third of upload-path hashing CPU.

Additionally, `ChunkData::hash()` runs synchronously on the tokio worker
thread. A 256 KiB SHA-256 is ~100 µs — fine in isolation, but under
`max-concurrent-chunk-uploads` parallelism it adds scheduler latency for
latency-sensitive narinfo requests being served by the same runtime. Consider
`tokio::task::spawn_blocking` (or a small rayon pool) for chunk hashing, or
at minimum `block_in_place`.

**Impact: medium** (upload CPU; also protects download tail latency under load) · **Effort: low-medium** · **Risk: low-medium** (must keep verification semantics for the untrusted `Stream` variant)

## 6. Use larger read buffers when streaming NARs

**Files:** `server/src/api/binary_cache.rs:222` and `:252`,
`server/src/storage/local.rs` (download path)

`ReaderStream::new` defaults to a 4 KiB buffer. NAR downloads stream through
it, so a 100 MiB NAR becomes ~25,000 tiny poll/allocation iterations (and for
the local backend, ~25,000 small `read(2)` syscalls). Switch to
`ReaderStream::with_capacity(stream, 64 * 1024)` (or 256 KiB, matching the
average chunk size) in both `get_nar` branches. Same consideration applies to
any `tokio::io::copy` in the proof-of-possession paths (uses 8 KiB
internally; `copy_buf` with a sized `BufReader` avoids it).

**Impact: medium** (download throughput/CPU, especially local storage backend) · **Effort: trivial** · **Risk: low**

## 7. Raise and make configurable the chunk prefetch depth for NAR reassembly

**File:** `server/src/api/binary_cache.rs:263` (`merge_chunks(…, 2)`, has a
`TODO: Make num_prefetch configurable`)

Multi-chunk NARs are reassembled by streaming chunks from storage with a
prefetch depth of only 2. With ~256 KiB chunks and S3 first-byte latency of
30–100 ms, the pipeline can stall between chunks and effective download
throughput caps at a few MiB/s per request even when bandwidth is plentiful.
Make it a config option and raise the default (4–8; each prefetched chunk
costs at most `chunking.max-size` bytes of buffering, so the memory budget
stays modest and calculable).

**Impact: medium** (download throughput of chunked NARs on S3) · **Effort: low** · **Risk: low**

## 8. Cache hot per-request lookups: cache row, parsed keypair, signatures

**Files:** `server/src/access/http.rs` (`auth_cache`),
`server/src/database/entity/cache.rs:61`, `server/src/api/binary_cache.rs`

Every single request — narinfo, NAR, cache-info — performs a `find_cache` DB
query, and every narinfo response base64-decodes and re-parses the cache's
ed25519 keypair (`cache.keypair()`) before signing. A small in-process TTL
cache (e.g. `moka`, 5–30 s TTL) keyed by cache name for the `CacheModel` +
parsed `NixKeypair` removes one query and one keypair parse per request. The
short TTL bounds staleness for permission/visibility changes; deletion
already soft-deletes so a few seconds of lag is acceptable (make the TTL
configurable, 0 = off, for operators who disagree). If narinfo throughput
ever matters beyond that, the computed signature itself can be cached per
object since the fingerprint is deterministic.

**Impact: low-medium** (per-request latency and DB QPS; grows with traffic) · **Effort: medium** · **Risk: medium** (staleness semantics around auth need care)

## 9. Tune the default compression level (or document the trade-off)

**File:** `server/src/config.rs:626` (`Zstd => Precise(8)`)

The default is zstd level 8. Zstd's sweet spot for on-the-fly compression is
usually level 3 (~2–3× faster compression for a ratio typically within a few
percent on binary artifacts). Since compression runs inline in the upload
path, level 8 directly limits per-chunk upload throughput and burns CPU that
competes with downloads. Benchmark on representative NARs; if the ratio delta
is small, lower the default to 3 and document level 8+ as an "archival
storage-cost" choice. (Also worth documenting that `xz` should be avoided for
performance-sensitive deployments.)

**Impact: low-medium** (upload CPU/throughput; zero code risk — it's a default) · **Effort: trivial** · **Risk: low**

## 10. Make GC storage-deletion concurrency configurable

**File:** `server/src/gc.rs:251` (`Semaphore::new(20)`, has a `TODO`)

Orphan-chunk deletion issues at most 20 concurrent `DeleteObject` calls. For
deployments that accumulate large GC backlogs (S3 handles far more, and
supports `DeleteObjects` batching of 1,000 keys per call), GC passes can take
hours. Expose the concurrency as config and/or use the S3 batch-delete API in
the S3 backend. Purely an operational-throughput win for GC, which runs
off the request path.

**Impact: low** (GC wall time only) · **Effort: low** · **Risk: low**

---

## Suggested sequencing

Items 1, 2, 6, 7, and 9 are small, independent, low-risk changes that mostly
target the download path — they can land as a first batch and are easy to
verify with the existing integration-test matrix (`sqlite,postgres` ×
`local,garage`). Items 4 and 5 are a second batch on the upload path. Item 8
follows once there's traffic data to justify it. Item 3 is a separate
protocol-design effort and by far the largest win for CI push workloads.

Before starting, it's worth adding a micro-benchmark or load-test harness
(e.g. `oha`/`vegeta` against a seeded local instance) so each change's effect
is measured rather than assumed.
