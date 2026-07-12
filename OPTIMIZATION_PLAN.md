# Attic Server Memory Optimization Plan

**Goal:** Stop OOM crashes of the attic server pod in a low-RAM k3s cluster during
concurrent NAR uploads, by bounding peak memory under load.

**Diagnosis (validated):** No single code path buffers a whole NAR in memory — the
streaming pipeline (hash → compress → storage) is sound. The OOMs come from
**fixed per-task costs multiplied by unbounded concurrency**, plus allocator
fragmentation that keeps RSS high after bursts:

- Nothing bounds how many uploads run at once; each chunked upload additionally
  fans out to 10 concurrent chunk-upload tasks (per request, not globally).
- The S3 multipart path reads the entire stream up front, spawning one task per
  8 MiB part with no bound — a multi-GB unchunked NAR can hold GBs of buffers.
- glibc malloc fragments under multi-threaded `BytesMut` churn and rarely returns
  freed memory to the OS (the published container image is glibc, not musl).
- The SQLite branch hardcodes `pragma mmap_size = 30000000000` (~28 GiB), which
  inflates cgroup page-cache accounting under memory limits.

Each step below is one commit, implemented by a Sonnet agent and adversarially
reviewed by an Opus agent before moving to the next. Ordering matters: steps 1
and 2 remove the unbounded-concurrency root causes; later steps are independent
hardening.

---

## Step 1 — Global upload concurrency budget (was findings B + D, co-designed)

**Files:** `server/src/api/v1/upload_path.rs`, `server/src/lib.rs`, `server/src/config.rs`

Today the chunk-upload semaphore (`CONCURRENT_CHUNK_UPLOADS = 10`,
`upload_path.rs:53`) is created *inside each request*, so aggregate concurrency is
`10 × concurrent uploads`, and nothing limits concurrent uploads at all
(`lib.rs:250-260` has no concurrency layer).

**Change:**
1. Add config options under `[api]` (or a new `[upload]` section):
   - `max-concurrent-uploads` (global cap on simultaneous `upload_path` requests;
     default: unlimited to preserve current behavior, documented for low-RAM pods)
   - `max-concurrent-chunk-uploads` (global, replaces the per-request constant;
     default: 10 — note this is a behavior change from 10-per-request to
     10-global, which is the intent)
2. Hold both as `Semaphore`s in `StateInner`; acquire a request permit at the top
   of the upload handlers, and acquire chunk permits from the global semaphore at
   the existing acquisition point (`upload_path.rs:336`).
3. Do **not** add a request body size limit — NAR uploads are legitimately huge
   and streamed (validator explicitly rejected this).

**Why first:** highest-leverage fix against the actual failure mode; defines the
concurrency budget that later sizing decisions (steps 2–3) assume.

**Risk:** low. Care point: permits must be dropped on request completion/abort
(RAII permit guards handle this); avoid deadlock by never awaiting a chunk permit
while holding resources that block other requests' progress (same pattern as
today, just a shared semaphore).

## Step 2 — Bound S3 multipart part concurrency and right-size the initial buffer (was A + C, one commit)

**Files:** `server/src/storage/s3.rs` (`upload_file`, lines ~160–290)

The multipart loop (`s3.rs:233-267`) reads the whole input, spawning an
`upload_part` task per 8 MiB part with no bound, awaiting only at the final
`join_all`. If the reader outpaces S3, all parts sit in memory (a 2 GiB
unchunked NAR ≈ ~256 × 8 MiB ≈ 2 GiB resident). This is the worst single OOM
vector, triggered whenever a blob ≥ 8 MiB reaches `upload_file` (chunking
disabled, or `nar-size-threshold` not met).

**Change:**
1. Add a semaphore bounding in-flight parts (default 4, constant is fine; the
   validator confirmed part ordering survives because `part_number` is assigned
   sequentially and results are collected in spawn order). **The permit must be
   acquired *before* `read_chunk_async` reads the next part** — otherwise the
   stream still races ahead and buffers everything.
2. While in this function: stop allocating a full 8 MiB `BytesMut` for every
   call (most calls are ≤256 KiB CDC chunks). Read into a modest initial buffer
   and grow/continue into the multipart path only if it fills. **Must preserve
   the existing PutObject-vs-multipart decision**, which currently keys off
   "did the first read fill the 8 MiB buffer". (Impact of this half is modest —
   `with_capacity` doesn't commit pages — but it reduces allocator pressure and
   is naturally done in the same edit.)

**Risk:** medium — touches upload correctness for the multipart path. Review
must verify: part ordering, ETag collection, empty-stream and exactly-8 MiB edge
cases, and that PutObject still fires for small blobs.

## Step 3 — Switch the global allocator to jemalloc (was F)

**Files:** `server/Cargo.toml`, `server/src/main.rs` (and the `atticadm` binary if separate)

No `#[global_allocator]` is set anywhere. glibc malloc fragments badly under
attic's multi-threaded variable-size `Bytes`/`BytesMut` churn and rarely returns
freed arenas to the OS — the classic "RSS climbs under load and never comes
down" container pattern. The validator confirmed the published container image
(`attic-server-image` → `crane.nix` standard stdenv) is **glibc, dynamically
linked**, so `tikv-jemallocator` builds cleanly.

**Change:** add `tikv-jemallocator` as a dependency of the server crate and set
it as `#[global_allocator]` in the server binary. Configure background purging
(e.g. `background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000` via
`malloc_conf`) so freed memory returns to the OS promptly in a memory-capped pod.

**Risk:** low-medium — dependency + build change; must verify the nix/crane
build still succeeds (`nix build .#attic-server` at minimum).

## Step 4 — Make SQLite `mmap_size` configurable with a sane default (was E)

**Files:** `server/src/lib.rs:136-144`, `server/src/config.rs`

The SQLite branch hardcodes `pragma mmap_size = 30000000000` (~28 GiB). Under
cgroup memory limits, the mmap'd DB pages are charged to the pod's page cache
and push it toward the OOM threshold during lookup-heavy bursts.

**Change:** add a `database.mmap-size` (or similar) config option applied only
for SQLite URLs, defaulting to a modest value (proposed: 512 MiB). Keep the
other pragmas as-is.

**Risk:** low. Behavior change for huge SQLite DBs (slightly slower reads
beyond the mmap window) — acceptable and overridable.

## Step 5 — Expose database connection pool settings (was G)

**Files:** `server/src/config.rs` (`DatabaseConfig`), `server/src/lib.rs:125`

`Database::connect` is called with a bare URL, so the sqlx pool silently
defaults to 10 max connections with no way to tune down (or up). Not a major
OOM contributor, but a missing lever for constrained pods.

**Change:** add optional `max-connections`, `min-connections`, `idle-timeout`
fields to `DatabaseConfig`, threaded through `sea_orm::ConnectOptions`. Defaults
match current behavior (no change unless set).

**Risk:** low, purely additive.

## Step 6 — Paginate/limit GC orphan-chunk batches (was H)

**Files:** `server/src/gc.rs:178-226`

On Postgres, one GC pass loads up to 65,535 `chunk::Model` rows into a `Vec` at
once, and GC runs in-process alongside the API server — worst case coinciding
with an upload burst. Low impact (a few MB), but a cheap hardening.

**Change:** process orphan chunks in smaller sub-batches (e.g. loop with a
1,000-row limit until exhausted) instead of one large fetch. Keeps within sqlx
bind-parameter caps by construction.

**Risk:** low. Must preserve the existing delete semantics per batch.

---

## Rejected / dropped findings

- **Request body size limit** — rejected by validator: would break legitimate
  large NAR uploads (they are streamed, not buffered).
- **Release profile tuning (`lto`, `codegen-units`, `strip`)** — the deployed
  container already gets `lto=fat`/`codegen-units=1` via `crane.nix` env vars,
  and this doesn't reduce runtime RSS anyway. `panic="abort"` would break
  `CatchPanicLayer`. Dropped.
- **`chunk_stream` per-chunk `BytesMut(max_size)` allocation** — bounded per
  request at the default 256 KiB `max-size`; informational only. Dropped.
- **"8 MiB reserved per chunk = 8 MiB RSS"** — corrected: `BytesMut::with_capacity`
  commits pages lazily; the real cost is allocator/fragmentation pressure,
  addressed by steps 2 (buffer sizing) and 3 (jemalloc).

## Verification (per step)

- `cargo check`/`cargo test` for the workspace (or `cargo test -p attic-server`).
- Step 3 additionally: `nix build .#attic-server` to confirm the container
  build path still compiles.
- Steps 1–2: reviewer specifically checks semaphore acquisition ordering
  (no deadlock, permit before read), part ordering, and error/abort paths
  releasing permits.

## Deployment notes (assumptions — please correct if wrong)

- Assumed storage backend: **S3-compatible** (steps 2 sizing) — if you use the
  local backend, step 2 still lands but won't affect you.
- Assumed database: **SQLite** (step 4) — if Postgres, step 4 is inert for you
  but still worth landing; step 5/6 become more relevant.
- After deploying, set `max-concurrent-uploads` (step 1) to a value sized for
  your pod's memory limit (suggested starting point for a 256–512 MiB pod: 2–4).
