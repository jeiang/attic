# AGENTS.md — `client` crate

Guidance for AI coding agents working in this crate. Package name is
`attic-client` (`client/Cargo.toml`); it builds one binary, `attic`
(`src/main.rs`).

## Purpose

This crate is the `attic` CLI: log into an Attic server, manage caches,
push store paths/closures to a binary cache, and wire the result into
`nix.conf`/`netrc` so Nix can substitute from it. It's a thin HTTP client
over the `server` crate's API, built on shared types/logic from the `attic`
crate (`../attic`, path dependency).

## CLI command map

Entry point `src/cli.rs` (`clap` derive, dispatch in `run()`). One file per
subcommand under `src/command/`:

- `attic login <name> <endpoint> [token] [--oidc <provider>] [--set-default]`
  — `command/login.rs`. Stores/updates a server entry in the config file;
  `--oidc` exchanges a provider ID token for an Attic token via `oidc.rs`.
- `attic use <cache>` — `command/use.rs`. Fetches a cache's config and edits
  local `nix.conf` (substituter + trusted key) and `netrc` (token).
- `attic push <cache> [paths...] [--stdin] [--no-closure] [-j N]
  [--ignore-upstream-cache-filter]` — `command/push.rs`. Drives the upload
  pipeline in `push.rs`.
- `attic watch-store <cache> [-j N] ...` — `command/watch_store.rs`. Watches
  the Nix store dir (`notify`, FSEvents on macOS) for lock-file removals
  (= newly valid paths) and streams them into a `PushSession`.
- `attic cache <create|configure|destroy|info|list>` — `command/cache.rs`:
  `create`/`configure` manage a cache's visibility/priority/retention/
  keypair/upstream-key-names; `destroy` requires typed confirmation unless
  `--no-confirm`; `info` prints a cache's config; `list` lists caches
  discoverable on a server (optional server-name arg, else default server —
  added recently, see `579f11a feat(client): add cache list command`).
- `attic get-closure <path> [--include-outputs]` — `command/get_closure.rs`,
  hidden (`#[clap(hide = true)]`). Debug command, like `nix-store -qR`.
- `attic gen-completions <shell>` — handled specially in `cli::run()` before
  `Opts::parse()` (it isn't a real `Command` variant).

## Module map

- `main.rs` — logging init + module wiring; strict `#![deny(unsafe_code,
  unused_imports/mut/vars in release, ...)]`.
- `cli.rs` — top-level `Opts`/`Command` enum, dispatch.
- `command/*.rs` — per-subcommand `clap::Parser` structs + `run()`.
- `api/mod.rs` — `ApiClient`: `reqwest`-based client for the server's
  `_api/v1/*` endpoints (cache CRUD, missing-paths, upload-path, OIDC
  provider discovery/exchange), plus upload retry logic and `ApiError`.
- `push.rs` — upload pipeline (`Pusher`, `PushSession`, `PushPlan`,
  `upload_path`); see below.
- `cache.rs` — `CacheRef`/`ServerName` parsing (`cachename` vs
  `server:cachename`); re-exports `attic::cache::CacheName`.
- `config.rs` — client config file (servers, tokens, default server).
- `nix_config.rs` / `nix_netrc.rs` — line-oriented, comment-preserving
  editors for Nix's `nix.conf` and `netrc`.
- `oidc.rs` — OIDC login flows for `attic login --oidc`.
- `version.rs` — `ATTIC_DISTRIBUTOR` build-time const, used in User-Agent.

## Config handling

- Client config: `$XDG_CONFIG_HOME/attic/config.toml` (TOML), deserialized
  as `ConfigData { default_server, servers: HashMap<ServerName,
  ServerConfig> }`. `ServerConfig { endpoint, token }` where `token` is
  untagged `ServerTokenConfig::Raw { token }` or `File { token_file }`
  (read + trimmed at use time).
- Auto-saved on mutation: `Config::as_mut()` returns a `ConfigWriteGuard`
  whose `Drop` impl calls `save()` (mode `0600`). This is how `attic login`
  persists changes.
- Nix's own `nix.conf`/`netrc` live under
  `xdg::BaseDirectories::with_prefix("nix")` and are edited by `NixConfig`
  / `NixNetrc`, which parse into line structures and only touch known keys
  to preserve user formatting/comments — don't replace with a generic
  TOML/INI round-trip.

## Push pipeline overview

All in `push.rs`:

- `Pusher` — fixed pool of async workers (`PushConfig::num_workers`, CLI
  `-j/--jobs`, default 5) reading `ValidPathInfo` off an `async_channel`.
  Used when the full path set is known upfront (`attic push`, no `--stdin`).
- `PushPlan::plan()` — computes the closure (unless `--no-closure`), fetches
  `ValidPathInfo` per path, drops paths signed by an upstream cache
  (`cache_config.upstream_cache_key_names`, bypass with
  `--ignore-upstream-cache-filter`), then calls the server's
  `get-missing-paths` to skip paths already present remotely.
- `PushSession` — wraps a `Pusher` to accept a live `StorePath` stream
  (`attic watch-store`, `attic push --stdin`). Batches for up to 2s idle or
  10s total before planning, and remembers already-queued path hashes
  (`known_paths`) across batches.
- `upload_path()` — builds `UploadPathNarInfo`; sends it as an HTTP header
  or, if it's ≥ 4 KiB (`NAR_INFO_PREAMBLE_THRESHOLD`, `api/mod.rs`) or
  `--force-preamble`, as a body preamble; streams the NAR via
  `store.nar_from_path()` through `NarStreamProgress` (drives the
  `indicatif` bar) to `PUT _api/v1/upload-path`.
- Retries (`api/mod.rs`): `ApiClient::upload_path_with_retry` retries up to
  `UPLOAD_MAX_ATTEMPTS = 3` with full-jitter backoff (`UPLOAD_BACKOFF_BASE
  = 500ms`, cap `5s`, honors `Retry-After`) only for transport errors or
  HTTP 408/429/499/500/502/503/504. Each retry re-invokes the caller's
  closure so a fresh NAR stream is built per attempt — never reuse a
  consumed stream.
- Concurrency knobs: `PushConfig::num_workers` (`-j`, must be > 0),
  `UPLOAD_MAX_ATTEMPTS`/`UPLOAD_BACKOFF_*` in `api/mod.rs`, and the 2s/10s
  batching windows hardcoded in `PushSession::worker`.

## Auth

- Token auth: `attic login <name> <endpoint> <token>` stores the token in
  `config.toml`; sent as `Authorization: bearer <token>`
  (`build_http_client`, `api/mod.rs`).
- OIDC (`attic login ... --oidc <provider>`, `oidc.rs`): fetches the
  server's advertised providers (`_api/v1/auth/oidc/providers`), then:
  - `AuthorizationCodePkce` — local `TcpListener` on `127.0.0.1:0`, opens
    the provider's auth URL in the browser (PKCE + state + nonce), waits
    up to 300s for the redirect callback, exchanges the code, validates
    the nonce in the returned JWT.
  - `GithubActions` — reads `ACTIONS_ID_TOKEN_REQUEST_URL`/`_TOKEN` env
    vars (needs `permissions.id-token: write` in the workflow).
  - Either way the ID token is exchanged for an Attic token via
    `_api/v1/auth/oidc/exchange`.
- `attic use` copies the server's token into the Nix `netrc` so Nix's own
  substituter fetches are authenticated too.

## Build / run / test

From the repo root (Cargo workspace):

- Build: `cargo build -p attic-client`
- Run: `cargo run -p attic-client -- <args>`
- Test: `cargo test -p attic-client` (covers config/nix.conf/netrc
  round-trips, retry/backoff math, OIDC nonce validation)
- Format check (CI gate, `just ci-rustfmt`): `cargo fmt --check`
- Depends on `attic` (`../attic`, path dep) for `NixStore`/`StorePath`/
  `ValidPathInfo` (FFI bindings to `libnixstore`), `CacheName`, the
  `attic::api::v1::*` wire types, and `AtticResult`. Building generally
  needs a Nix dev shell (`nix develop`), not a bare `cargo build`, since
  `attic` links against `libnixstore`.

## Conventions & gotchas

- Subcommands are matched via `EnumAsInner` (`opts.command.as_push()
  .unwrap()`), not a generic downcast — adding one means updating `Command`
  in both `cli.rs` and `command/mod.rs` plus the `match` in `cli::run()`.
- Hidden flags/commands (`#[clap(hide = true)]`: `get-closure`,
  `--store-dir`, `--force-preamble`) are internal/experimental, not
  user-facing surface.
- `main.rs` denies unused imports/vars/mut only in release builds — code
  that compiles under `cargo build` can still fail `cargo build --release`.
- `PushSession::queue_many`/`flush` use `send_blocking` so they're callable
  from sync callbacks (e.g. the `notify` watcher in `watch_store.rs`), but
  will error if the worker task already exited.
- Errors are `anyhow::Error` at the CLI/command layer; the `attic` crate
  uses `AtticResult`/`AtticError` — conversions happen at the boundary
  (`push.rs`, `NarStreamProgress`).
