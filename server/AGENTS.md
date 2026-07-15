# AGENTS.md — attic-server

Guidance for AI coding agents working in `./server`: the Nix binary cache
server — an axum HTTP API, a garbage collector, a SeaORM database
(PostgreSQL or SQLite), and a pluggable storage backend (local disk or S3).

## Purpose

- Implements the [Nix HTTP Binary Cache API](https://github.com/fzakaria/nix-http-binary-cache-api-spec)
  (`nix-cache-info`, `.narinfo`, `.nar`) so `nix`/`nix-store` can substitute
  directly from it.
- Implements Attic's internal `/_api/v1/*` API used by the `attic` CLI
  (`../client`) to manage caches and upload/query NARs.
- Exchanges verified OIDC ID tokens (e.g. from GitHub Actions) for
  short-lived Attic JWTs, so CI doesn't need a static secret.

## Crate layout

- `src/main.rs` — `atticd` binary: CLI, run-mode dispatch, signal handling.
- `src/adm/` — `atticadm` binary (offline `make-token` command).
- `src/lib.rs` — global `StateInner`/`RequestStateInner`, router assembly,
  `run_api_server`, `run_migrations`.
- `src/api/` — HTTP handlers (binary cache API, v1 API, OIDC).
- `src/access/` — re-exports the `attic-token` crate + axum auth middleware
  (`access/http.rs`).
- `src/database/` — SeaORM entities (`database/entity/`), migrations
  (`database/migration/`), and the `AtticDatabase` query trait.
- `src/storage/` — storage backend abstraction (`local.rs`, `s3.rs`).
- `src/gc.rs` — garbage collection.
- `src/config.rs` / `src/config-template.toml` — config schema + OOBE template.
- `src/oobe.rs` — first-run setup (generates SQLite + local-storage config).
- `src/narinfo/`, `src/nix_manifest/` — `.narinfo` model and Nix's custom
  (non-JSON) serialization format.
- `src/compression.rs` — streaming brotli/zstd/xz during upload.
- `src/alloc.rs` — jemalloc global allocator (non-MSVC targets).

## Run modes & entrypoints

`atticd --mode <mode>` (`src/main.rs`):
- `monolithic` (default) — migrations, then GC loop + API server together;
  typical self-hosted deployment.
- `api-server` / `garbage-collector` — split-deployment components.
- `db-migrations` — apply pending migrations, exit.
- `garbage-collector-once` — one GC pass, exit.
- `check-config` — validate config, exit.

`atticadm make-token` mints a JWT offline using the config's signing key.

## API surface

Binary Cache API (`src/api/binary_cache.rs`), under `/{cache}/...`:

| Method | Path | Handler |
|---|---|---|
| GET | `/{cache}/nix-cache-info` | `get_nix_cache_info` |
| GET | `/{cache}/{storePathHash}.narinfo` | `get_store_path_info` |
| GET | `/{cache}/nar/{storePathHash}.nar` | `get_nar` (S3 redirect for single-chunk NARs, else streams/reassembles chunks) |

Internal v1 API (`src/api/v1/mod.rs`), under `/_api/v1/...`:

| Method | Path | Handler file |
|---|---|---|
| POST | `get-missing-paths` | `get_missing_paths.rs` |
| PUT | `upload-path` | `upload_path.rs` |
| GET | `cache-config` | `cache_config.rs::list_caches` |
| GET | `/{cache}/attic-cache-info`, `cache-config/{cache}` | `cache_config.rs::get_cache_config` |
| POST | `cache-config/{cache}` | `cache_config.rs::create_cache` |
| PATCH | `cache-config/{cache}` | `cache_config.rs::configure_cache` |
| DELETE | `cache-config/{cache}` | `cache_config.rs::destroy_cache` |

OIDC (`src/api/oidc.rs`, wired in `src/api/mod.rs`): GET
`auth/oidc/providers`, POST `auth/oidc/exchange`.

Middleware order (auth, request-id, host-restriction, visibility header) is
assembled in `src/lib.rs::run_api_server` — see `src/middleware.rs`.

## Data model & dedup

`cache` → `object` (one per store path per cache) → `nar` (content-addressed
archive, deduplicated server-wide, not per-cache) → `chunkref` → `chunk`
(content-addressed compressed blob). NARs are deduplicated by NAR hash, and
each NAR is additionally split into content-addressed chunks (FastCDC, see
`attic::chunking`) so even partially-similar NARs share storage;
`holders_count` on `nar`/`chunk` protects content mid-upload from GC.
`chunk.remote_file` (`Json<RemoteFile>`) points at local or S3 storage.
`NarState`/`ChunkState` track `PendingUpload` → `Valid`/`ConfirmedDeduplicated`
→ `Deleted`.

## AuthZ model

Stateless JWT auth, implemented in the sibling `attic-token` crate
(`../token/src/lib.rs`), consumed via `src/access/http.rs`.

- `apply_auth` middleware extracts a bearer/basic-auth token, verifies it
  against `config.jwt.signing_config` (RS256 keypair, RS256 verify-only
  pubkey, or HS256 secret), and stashes it in per-request `AuthState`.
- The JWT's `https://jwt.attic.rs/v1` claim maps cache-name *patterns*
  (`*` wildcards supported) to a `CachePermission` bitset: `pull`, `push`,
  `delete`, `create_cache`, `configure_cache`, `configure_cache_retention`,
  `destroy_cache`. First matching pattern wins (direct name match tried
  first, then insertion order) — wildcard rule order in a token matters.
- Handlers call `req_state.auth.auth_cache(db, &cache_name, |cache, perm| {
  perm.require_pull()? ... })`, or `get_permission_for_cache` when the cache
  row isn't loaded yet (e.g. `create_cache`). Public caches implicitly grant
  `pull` to everyone.
- Discovery-leak prevention: a token with *no* permission on a cache gets a
  generic 401 rather than a response that reveals whether the cache exists
  (`can_discover()` / `NoDiscoveryPermission` vs `PermissionDenied`).
- OIDC (`config.oidc.providers`) trades a verified ID token for a short-lived
  Attic JWT, gated by claim-matching `rules` that grant specific cache
  permissions.

## Storage backends

`src/storage/mod.rs` defines `StorageBackend` (`enum_dispatch` over
`LocalBackend`/`S3Backend`): `upload_file`, `delete_file(_db)`,
`download_file_db`, `make_db_reference`. `RemoteFile` is the DB-persisted
reference (`LocalRemoteFile`, `S3RemoteFile`, plus an `Http` variant for
tests). Local (`storage/local.rs`) stores under a configured directory with
a `VERSION` file for layout migrations. S3 (`storage/s3.rs`) supports custom
endpoints (Minio/R2), multipart upload (`multipart-concurrency`), and
presigned-URL redirects for single-chunk downloads.

## Config

`config::load_config` (`src/config.rs`) resolves, in order: `--config`/`-f`
path → `ATTIC_SERVER_CONFIG_BASE64` env (base64 TOML, for platforms like
Fly.io) → XDG path (`$XDG_CONFIG_HOME/attic/server.toml`), auto-created by
the OOBE flow (`src/oobe.rs`) on first `monolithic` run with no config
(SQLite + local storage under `$XDG_DATA_HOME/attic`).

Env var overrides (used when the TOML key is absent): `ATTIC_SERVER_DATABASE_URL`,
`ATTIC_SERVER_TOKEN_HS256_SECRET_BASE64`, `ATTIC_SERVER_TOKEN_RS256_SECRET_BASE64`,
`ATTIC_SERVER_TOKEN_RS256_PUBKEY_BASE64`. Exactly one JWT signing option must
resolve (config `[jwt.signing]` or env), else `atticd` panics with a
descriptive message.

Full annotated schema (also the OOBE template) is `src/config-template.toml`.
`validate_config` enforces cross-field invariants (non-zero concurrency
limits, S3 config, unique/non-empty OIDC rules, `min <= max` connections).
Run `atticd --mode check-config` to validate without starting the server.

## Database & migrations

SeaORM targets PostgreSQL (production) and SQLite (dev/tests); entities in
`src/database/entity/{cache,nar,chunk,chunkref,object}.rs`. To add a
migration: create `src/database/migration/mYYYYMMDD_NNNNNN_description.rs`
implementing `MigrationTrait`, register it in `src/database/migration/mod.rs`
(both the `mod` decl and appended to the `migrations()` vec — never reorder
or remove past entries, this is shared production history), update the
matching entity `Model`, then run
`cargo run -p attic-server --bin atticd -- --mode db-migrations` (with
`ATTIC_SERVER_DATABASE_URL` set) to apply it locally.

## GC

`src/gc.rs`, run periodically (`garbage-collection.interval`, default 12h; 0
disables the loop but not `garbage-collector-once`) in three passes:
1. Time-based: delete `object` rows past a cache's `retention_period` (or
   the global default) whose `last_accessed_at` is also stale.
2. Orphan NARs: delete `nar` rows with no referencing `object` and
   `holders_count == 0`.
3. Orphan chunks: transition unreferenced `chunk` rows to `Deleted`, delete
   backing storage files in bounded batches (500 SQLite / 1000 Postgres),
   then delete the rows. Storage-deletion failures are tolerated; affected
   chunks stay `Deleted` and retry next pass.

## Build / run / test

Binaries: `atticd` (`src/main.rs`), `atticadm` (`src/adm/main.rs`); package
`attic-server`.

```sh
cargo build -p attic-server
cargo test -p attic-server
```

SQLite + local storage is enough for dev — no Postgres/Minio required:
```sh
export ATTIC_SERVER_DATABASE_URL="sqlite://$(pwd)/dev.db?mode=rwc"
cargo run -p attic-server --bin atticd -- --mode monolithic
```
Or run with no config and let OOBE generate one under XDG dirs. Any code
path touching `config::load_config` (including `check-config` and tests)
needs a resolvable JWT signing key (see Config above).

Unit tests are plain `#[test]`/`#[tokio::test]` (`access/tests.rs`,
`narinfo/tests.rs`, `nix_manifest/tests.rs`, inline tests in
`middleware.rs`/`config.rs`) — no external services needed. Real
end-to-end/substitution tests live under `../integration-tests/` and run via
Nix (`just ci-unit-tests`), not `cargo test`.

## Gotchas

- Migrations are append-only shared history — never edit/reorder a merged
  one; add a new one instead.
- Changing `[chunking]` values doesn't break anything but degrades dedup
  until the store "warms up" with new cutpoints (see `ChunkingConfig` docs).
- Changing `[jwt]` invalidates all previously issued tokens; the only
  "rotation" path is switching to `RS256VerifyOnly` for verify-only nodes.
- `soft-delete-caches = true` makes a destroyed cache's name permanently
  unavailable for reuse (row stays with `deleted_at` set).
- `api-endpoint`/`substituter-endpoint`, if unset, are synthesized from the
  client's `Host` header — fine for local dev, insecure/spoofable in
  production unless `allowed-hosts` is also set.
- Middleware layer order in `run_api_server` matters (`init_request_state`
  must run before handlers/middlewares that read `RequestState`) — don't
  reorder `.layer(...)` calls without checking dependencies.
