#![deny(
    asm_sub_register,
    deprecated,
    missing_abi,
    unsafe_code,
    unused_macros,
    unused_must_use,
    unused_unsafe
)]
#![deny(clippy::from_over_into, clippy::needless_question_mark)]
#![cfg_attr(
    not(debug_assertions),
    deny(unused_imports, unused_mut, unused_variables,)
)]

pub mod access;
mod api;
mod compression;
pub mod config;
pub mod database;
pub mod error;
pub mod gc;
mod middleware;
mod narinfo;
pub mod nix_manifest;
pub mod oobe;
mod storage;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    Router,
    extract::Extension,
    http::{Uri, uri::Scheme},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, SqlxSqliteConnector,
    query::Statement,
    sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::net::TcpListener;
use tokio::sync::OnceCell;
use tokio::sync::{Mutex, Semaphore};
use tokio::time;
use tokio_util::sync::CancellationToken;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use access::http::{AuthState, apply_auth};
use attic::cache::CacheName;
use config::{Config, StorageConfig};
use database::migration::{Migrator, MigratorTrait};
use error::{ErrorKind, ServerError, ServerResult};
use middleware::{assign_request_id, init_request_state, restrict_host, set_visibility_header};
use storage::{LocalBackend, S3Backend, StorageBackendImpl};

type State = Arc<StateInner>;
type RequestState = Arc<RequestStateInner>;

/// Global server state.
#[derive(Debug)]
pub struct StateInner {
    /// The Attic Server configuration.
    config: Config,

    /// Handle to the database.
    database: OnceCell<DatabaseConnection>,

    /// Handle to the storage backend.
    storage: OnceCell<Arc<StorageBackendImpl>>,

    /// Limits whole authenticated uploads across all requests when configured.
    upload_permits: Option<Arc<Semaphore>>,

    /// Limits chunk uploads to storage across all requests.
    chunk_upload_permits: Arc<Semaphore>,

    /// Cached JSON Web Key Sets for OIDC providers.
    oidc_keysets: Mutex<HashMap<String, CachedOidcKeyset>>,
}

/// An OIDC JSON Web Key Set cached until its next refresh.
#[derive(Debug)]
struct CachedOidcKeyset {
    refresh_at: Instant,
    keys: Vec<serde_json::Value>,
}

/// Request state.
#[derive(Debug)]
struct RequestStateInner {
    /// Request identifier returned to the client and included in request spans.
    request_id: Uuid,

    /// Auth state.
    auth: AuthState,

    /// The canonical API endpoint.
    api_endpoint: Option<String>,

    /// The canonical substituter endpoint.
    substituter_endpoint: Option<String>,

    /// The potentially-invalid Host header supplied by the client.
    host: String,

    /// Whether the client claims the connection is HTTPS or not.
    client_claims_https: bool,

    /// Whether the cache the client's interacting with is public.
    ///
    /// This is purely informational and used to add the `X-Attic-Cache-Visibility`.
    /// header in responses.
    public_cache: AtomicBool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RequestId(pub Uuid);

impl StateInner {
    async fn new(config: Config) -> State {
        let upload_permits = config
            .max_concurrent_uploads
            .map(|n| Arc::new(Semaphore::new(n)));
        let chunk_upload_permits = Arc::new(Semaphore::new(config.max_concurrent_chunk_uploads));

        Arc::new(Self {
            config,
            database: OnceCell::new(),
            storage: OnceCell::new(),
            upload_permits,
            chunk_upload_permits,
            oidc_keysets: Mutex::new(HashMap::new()),
        })
    }

    /// Returns a handle to the database.
    async fn database(&self) -> ServerResult<&DatabaseConnection> {
        self.database
            .get_or_try_init(|| async {
                if self.config.database.url.starts_with("sqlite:") {
                    return self.connect_sqlite().await;
                }

                let mut connect_options = ConnectOptions::new(self.config.database.url.clone());
                if let Some(max_connections) = self.config.database.max_connections {
                    connect_options.max_connections(max_connections);
                }
                if let Some(min_connections) = self.config.database.min_connections {
                    connect_options.min_connections(min_connections);
                }
                if let Some(idle_timeout) = self.config.database.idle_timeout {
                    connect_options.idle_timeout(idle_timeout);
                }

                Database::connect(connect_options)
                    .await
                    .map_err(ServerError::database_error)
            })
            .await
    }

    /// Connects to a SQLite database.
    ///
    /// The tuning pragmas are set as connect options so that every
    /// pooled connection gets them, not only the first one handed out.
    /// See <https://phiresky.github.io/blog/2020/sqlite-performance-tuning/>.
    async fn connect_sqlite(&self) -> ServerResult<DatabaseConnection> {
        use std::str::FromStr;

        let connect_options = SqliteConnectOptions::from_str(&self.config.database.url)
            .map_err(ServerError::database_error)?
            .busy_timeout(self.config.database.busy_timeout)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .pragma("temp_store", "memory")
            .pragma("mmap_size", self.config.database.mmap_size.to_string());

        let mut pool_options = SqlitePoolOptions::new();
        if let Some(max_connections) = self.config.database.max_connections {
            pool_options = pool_options.max_connections(max_connections);
        }
        if let Some(min_connections) = self.config.database.min_connections {
            pool_options = pool_options.min_connections(min_connections);
        }
        if let Some(idle_timeout) = self.config.database.idle_timeout {
            pool_options = pool_options.idle_timeout(idle_timeout);
        }

        let pool = pool_options
            .connect_with(connect_options)
            .await
            .map_err(ServerError::database_error)?;

        Ok(SqlxSqliteConnector::from_sqlx_sqlite_pool(pool))
    }

    /// Acquires the configured global upload permit, if enabled.
    async fn acquire_upload_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        match &self.upload_permits {
            Some(semaphore) => Some(
                semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("upload semaphore is never closed"),
            ),
            None => None,
        }
    }

    /// Returns a handle to the storage backend.
    async fn storage(&self) -> ServerResult<&Arc<StorageBackendImpl>> {
        self.storage
            .get_or_try_init(|| async {
                match &self.config.storage {
                    StorageConfig::Local(local_config) => {
                        let local = LocalBackend::new(local_config.clone()).await?;
                        Ok(Arc::new(local.into()))
                    }
                    StorageConfig::S3(s3_config) => {
                        let s3 = S3Backend::new(s3_config.clone()).await?;
                        Ok(Arc::new(s3.into()))
                    }
                }
            })
            .await
    }

    /// Sends periodic heartbeat queries to the database.
    async fn run_db_heartbeat(&self) -> ServerResult<()> {
        let db = self.database().await?;
        let stmt =
            Statement::from_string(db.get_database_backend(), "SELECT 'heartbeat';".to_string());

        loop {
            let _ = db.execute_raw(stmt.clone()).await;
            time::sleep(Duration::from_secs(60)).await;
        }
    }
}

impl RequestStateInner {
    /// Returns the base API endpoint for clients.
    ///
    /// The APIs encompass both the Attic API and the Nix binary
    /// cache API.
    fn api_endpoint(&self) -> ServerResult<String> {
        if let Some(endpoint) = &self.api_endpoint {
            Ok(endpoint.to_owned())
        } else {
            // Naively synthesize from client's Host header
            // For convenience and shouldn't be used in production!
            let uri = Uri::builder()
                .scheme(if self.client_claims_https {
                    Scheme::HTTPS
                } else {
                    Scheme::HTTP
                })
                .authority(self.host.to_owned())
                .path_and_query("/")
                .build()
                .map_err(ServerError::request_error)?;

            Ok(uri.to_string())
        }
    }

    /// Returns the Nix binary cache endpoint for clients.
    ///
    /// The binary cache endpoint may live on another host than
    /// the canonical API endpoint.
    fn substituter_endpoint(&self, cache: CacheName) -> ServerResult<String> {
        if let Some(substituter_endpoint) = &self.substituter_endpoint {
            Ok(format!("{}{}", substituter_endpoint, cache.as_str()))
        } else {
            Ok(format!("{}{}", self.api_endpoint()?, cache.as_str()))
        }
    }

    /// Indicates whether the cache the client is interacting with is public.
    fn set_public_cache(&self, public: bool) {
        self.public_cache.store(public, Ordering::Relaxed);
    }
}

/// The fallback route.
#[axum_macros::debug_handler]
async fn fallback(_: Uri) -> ServerResult<()> {
    Err(ErrorKind::NotFound.into())
}

/// Runs the API server until shutdown is requested.
pub async fn run_api_server(
    cli_listen: Option<SocketAddr>,
    config: Config,
    shutdown: CancellationToken,
) -> Result<()> {
    eprintln!("Starting API server...");

    let state = StateInner::new(config).await;

    let listen = if let Some(cli_listen) = cli_listen {
        cli_listen
    } else {
        state.config.listen.to_owned()
    };

    let rest = Router::new()
        .merge(api::get_router())
        .fallback(fallback)
        // middlewares
        .layer(axum::middleware::from_fn(apply_auth))
        .layer(axum::middleware::from_fn(set_visibility_header))
        .layer(axum::middleware::from_fn(init_request_state))
        .layer(axum::middleware::from_fn(restrict_host))
        .layer(Extension(state.clone()))
        .layer(TraceLayer::new_for_http())
        .layer(CatchPanicLayer::new())
        .layer(axum::middleware::from_fn(assign_request_id));

    eprintln!("Listening on {:?}...", listen);

    let listener = TcpListener::bind(&listen).await?;

    let server = axum::serve(listener, rest);

    let heartbeat_handle = if state.config.database.heartbeat {
        let state_clone = state.clone();
        Some(tokio::spawn(async move {
            let _ = state_clone.run_db_heartbeat().await;
        }))
    } else {
        None
    };

    let server_result = server
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await;

    if let Some(handle) = heartbeat_handle {
        handle.abort();
        let _ = handle.await;
    }

    server_result?;

    Ok(())
}

/// Runs database migrations.
pub async fn run_migrations(config: Config) -> Result<()> {
    eprintln!("Running migrations...");

    let state = StateInner::new(config).await;
    let db = state.database().await?;
    Migrator::up(db, None).await?;

    Ok(())
}
