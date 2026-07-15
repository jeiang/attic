# AGENTS.md — Attic

Attic is a self-hostable Nix binary cache server backed by S3-compatible or
local storage, with global deduplication (content-addressed NAR + chunk
stores), on-the-fly signing, multi-tenant caches, and LRU garbage collection.

## Workspace layout

Cargo workspace with four crates. Each has its own `AGENTS.md` with
crate-specific guidance — read it before working in that crate:

| Path      | Package        | What it is |
|-----------|----------------|------------|
| `attic/`  | `attic`        | Shared library: Nix store bindings, NAR handling, chunking, hashing, signing, API types. See [attic/AGENTS.md](attic/AGENTS.md). |
| `client/` | `attic-client` | The `attic` CLI: cache management, push pipeline, `nix.conf`/netrc editing, OIDC login. See [client/AGENTS.md](client/AGENTS.md). |
| `server/` | `attic-server` | `atticd` (axum API server / GC) and `atticadm` (admin CLI). SeaORM database, storage backends. See [server/AGENTS.md](server/AGENTS.md). |
| `token/`  | `attic-token`  | JWT token format: per-cache permission claims and pattern matching. See [token/AGENTS.md](token/AGENTS.md). |

Non-crate directories:

- `flake/` — flake-parts modules, auto-imported by `flake.nix` (packages,
  devshells, NixOS module wiring, integration tests, overlays, pinned Nix
  versions for testing).
- `nixos/atticd.nix` — the NixOS module for running the server.
- `integration-tests/` — NixOS VM end-to-end tests (see Testing below).
- `book/` — user documentation (mdBook, published to docs.attic.rs).
- `.github/workflows/` — CI: `build.yml` (Linux + macOS, pushes to a
  self-hosted Attic cache via GitHub OIDC), `lint.yml`, `book.yml`.

## Dev environment

- Enter the devshell with `nix develop` — it provides the Rust toolchain,
  clippy/rustfmt, `just`, `jq`, sqlite/postgres clients, and more.
  `flake-compat.nix` exists for non-flake entry points.
- The main non-obvious build prerequisite: the `attic` crate's default
  `nix_store` feature compiles C++ FFI bindings against `libnixstore`
  (Nix ~2.24, discovered via pkg-config, needs a C++23 compiler). Only the
  **client** actually uses this FFI; the **server** deliberately depends on
  `attic` without `nix_store` and never links libnixstore. The devshell
  provides the libraries — outside it, `attic`/`client` builds will fail at
  `attic/build.rs`.
- `just list` shows available recipes. `just with-nix <version> <cmd>` runs a
  command against an alternative pinned Nix version (used to test the
  `attic` crate's C++ FFI against multiple Nix releases).

## Build, lint, test

- Build everything: `cargo build` (or `nix build` for the packaged outputs).
- Per-crate: `cargo build -p attic|attic-client|attic-server|attic-token`.
- Format: `cargo fmt` — CI enforces `cargo fmt --check` (`just ci-rustfmt`).
  An `.editorconfig` checker also runs in CI.
- Unit tests: `cargo test` (per-crate with `-p`). CI runs them via
  `just ci-unit-tests <matrix>`, which builds `.#internalMatrix` test
  binaries with Nix.
- WASM compatibility: `attic` (with `--no-default-features -F chunking -F io`)
  and `token` must keep building for `wasm32-unknown-unknown`
  (`just ci-build-wasm`). Don't add default dependencies to those crates that
  break WASM.
- Integration tests (Linux only, NixOS VMs, exposed as flake checks):
  `nix build .#checks.x86_64-linux.basic-sqlite-local` etc. The matrix is
  database × storage = {sqlite, postgres} × {local, garage}, plus `stable-*`
  variants (tested against nixpkgs-stable), defined in
  `integration-tests/default.nix` and wired up in `flake/integration-tests.nix`.
  Run the relevant combination when touching server storage, database, or API
  behavior.

## Conventions

- Rust edition/workspace settings live in each crate's `Cargo.toml`; the
  workspace root only lists members (resolver = "2").
- Keep commits focused; CI must pass `fmt`, unit tests on Linux + macOS, and
  the WASM builds.
- User-facing behavior changes should update the book (`book/src`).
- The `attic` library is consumed by all other crates — check cross-crate
  impact (client, server, token) when changing shared types, and remember
  its WASM constraint. Its `api/` types are the wire-format contract between
  client and server.
- Access control: JWT cache-permission patterns are matched
  **first-match-wins in insertion order**, not most-specific-wins. Keep this
  in mind for anything touching auth or cache-name namespacing (details in
  [token/AGENTS.md](token/AGENTS.md)).
- Known issue: a standalone `cargo build -p attic-token` currently fails —
  `attic/src/lib.rs` gates `pub mod testing` only on unix while the module
  needs the optional `tokio` feature. Workspace-wide builds are unaffected
  (feature unification). See the gotcha in [token/AGENTS.md](token/AGENTS.md);
  don't "fix" it by adding features to `token/Cargo.toml`.
