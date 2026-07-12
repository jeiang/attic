//! Global allocator configuration.
//!
//! glibc's allocator fragments badly under attic's multi-threaded, variable-sized
//! `Bytes`/`BytesMut` churn (NAR chunking/streaming) and rarely returns freed
//! memory to the OS. This is the classic "RSS climbs under load and never comes
//! back down" pattern in memory-limited containers.
//!
//! jemalloc handles this workload much better. Runtime tuning (background
//! purging + aggressive decay so freed memory is returned to the OS promptly)
//! is baked in at build time via `JEMALLOC_SYS_WITH_MALLOC_CONF` in
//! `.cargo/config.toml`, rather than via a `malloc_conf` weak-symbol override
//! here — `tikv-jemalloc-sys` prefixes jemalloc's internal symbols by default
//! (see its `build.rs`), which makes a plain `#[export_name = "malloc_conf"]`
//! override unreliable across platforms/crate versions. The build-time flag is
//! read directly by the build script and passed to jemalloc's
//! `./configure --with-malloc-conf=...`, so it isn't affected by prefixing.
//!
//! jemalloc doesn't support MSVC, so this is gated to non-MSVC targets (Linux
//! and macOS, which is all we target/deploy on).

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
