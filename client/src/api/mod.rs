use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::time::{Duration, SystemTime};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use const_format::formatcp;
use futures::{
    future,
    stream::{self, StreamExt, TryStream, TryStreamExt},
};
use rand::RngExt;
use reqwest::{
    Body, Client as HttpClient, Response, StatusCode, Url,
    header::{AUTHORIZATION, CONTENT_LENGTH, HeaderMap, HeaderValue, RETRY_AFTER, USER_AGENT},
};
use serde::{Deserialize, Serialize};
use tokio::time;

use crate::config::ServerConfig;
use crate::version::ATTIC_DISTRIBUTOR;
use attic::api::v1::cache_config::{CacheConfig, CreateCacheRequest, ListCachesResponse};
use attic::api::v1::get_missing_chunks::{GetMissingChunksRequest, GetMissingChunksResponse};
use attic::api::v1::get_missing_paths::{GetMissingPathsRequest, GetMissingPathsResponse};
use attic::api::v1::upload_path::{
    ATTIC_NAR_INFO, ATTIC_NAR_INFO_PREAMBLE_SIZE, UploadPathNarInfo, UploadPathResult,
};
use attic::cache::CacheName;
use attic::hash::Hash;
use attic::nix_store::StorePathHash;

/// The User-Agent string of Attic.
const ATTIC_USER_AGENT: &str =
    formatcp!("Attic/{} ({})", env!("CARGO_PKG_NAME"), ATTIC_DISTRIBUTOR);

/// The size threshold to send the upload info as part of the PUT body.
const NAR_INFO_PREAMBLE_THRESHOLD: usize = 4 * 1024; // 4 KiB
const UPLOAD_MAX_ATTEMPTS: usize = 3;
const UPLOAD_BACKOFF_BASE: Duration = Duration::from_millis(500);
const UPLOAD_BACKOFF_CAP: Duration = Duration::from_secs(5);

/// The Attic API client.
#[derive(Debug, Clone)]
pub struct ApiClient {
    /// Base endpoint of the server.
    endpoint: Url,

    /// An initialized HTTP client.
    client: HttpClient,
}

/// An API error.
#[derive(Debug)]
pub enum ApiError {
    Structured {
        error: StructuredApiError,
        status: StatusCode,
        retry_after: Option<Duration>,
    },

    Unstructured {
        status: StatusCode,
        message: String,
        retry_after: Option<Duration>,
    },
}

#[derive(Debug)]
pub(crate) enum UploadAttemptError {
    Transport(anyhow::Error),
    Http(ApiError),
}

#[derive(Debug)]
pub(crate) struct UploadAttempt<T> {
    pub value: T,
    pub attempts: usize,
}

#[derive(Debug)]
pub(crate) struct UploadPathRequest {
    endpoint: Url,
    metadata: UploadPathMetadata,
    content_length: u64,
}

#[derive(Debug)]
enum UploadPathMetadata {
    Header(HeaderValue),
    Preamble(Bytes),
}

#[derive(Debug, Clone, Deserialize)]
pub struct StructuredApiError {
    #[allow(dead_code)]
    code: u16,
    error: String,
    message: String,
}

/// A server-advertised OIDC login provider.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcProvider {
    pub name: String,
    pub display_name: String,
    pub mode: OidcProviderMode,
    pub issuer: String,
    pub audience: String,
    pub authorization_endpoint: Option<String>,
    pub token_endpoint: Option<String>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OidcProviderMode {
    AuthorizationCodePkce,
    GithubActions,
}

#[derive(Deserialize)]
struct OidcProvidersResponse {
    providers: Vec<OidcProvider>,
}

#[derive(Serialize)]
struct OidcExchangeRequest<'a> {
    provider: &'a str,
    id_token: &'a str,
}

#[derive(Deserialize)]
struct OidcExchangeResponse {
    access_token: String,
}

impl ApiClient {
    /// Fetches the OIDC providers configured by an Attic server.
    pub async fn oidc_providers(endpoint: &str) -> Result<Vec<OidcProvider>> {
        let endpoint = Url::parse(endpoint)?.join("_api/v1/auth/oidc/providers")?;
        let response = build_http_client(None).get(endpoint).send().await?;
        if response.status().is_success() {
            Ok(response.json::<OidcProvidersResponse>().await?.providers)
        } else {
            Err(ApiError::try_from_response(response).await?.into())
        }
    }

    /// Exchanges a provider ID token for a short-lived Attic token.
    pub async fn exchange_oidc_token(
        endpoint: &str,
        provider: &str,
        id_token: &str,
    ) -> Result<String> {
        let endpoint = Url::parse(endpoint)?.join("_api/v1/auth/oidc/exchange")?;
        let response = build_http_client(None)
            .post(endpoint)
            .json(&OidcExchangeRequest { provider, id_token })
            .send()
            .await?;
        if response.status().is_success() {
            Ok(response.json::<OidcExchangeResponse>().await?.access_token)
        } else {
            Err(ApiError::try_from_response(response).await?.into())
        }
    }

    pub fn from_server_config(config: ServerConfig) -> Result<Self> {
        let client = build_http_client(config.token()?.as_deref());

        Ok(Self {
            endpoint: Url::parse(&config.endpoint)?,
            client,
        })
    }

    /// Sets the API endpoint of this client.
    pub fn set_endpoint(&mut self, endpoint: &str) -> Result<()> {
        self.endpoint = Url::parse(endpoint)?;
        Ok(())
    }

    /// Lists the caches discoverable by the caller.
    pub async fn list_caches(&self) -> Result<ListCachesResponse> {
        let endpoint = self.endpoint.join("_api/v1/cache-config")?;
        let res = self.client.get(endpoint).send().await?;

        if res.status().is_success() {
            let caches = res.json().await?;
            Ok(caches)
        } else {
            let api_error = ApiError::try_from_response(res).await?;
            Err(api_error.into())
        }
    }

    /// Returns the configuration of a cache.
    pub async fn get_cache_config(&self, cache: &CacheName) -> Result<CacheConfig> {
        let endpoint = self
            .endpoint
            .join("_api/v1/cache-config/")?
            .join(cache.as_str())?;

        let res = self.client.get(endpoint).send().await?;

        if res.status().is_success() {
            let cache_config = res.json().await?;
            Ok(cache_config)
        } else {
            let api_error = ApiError::try_from_response(res).await?;
            Err(api_error.into())
        }
    }

    /// Creates a cache.
    pub async fn create_cache(&self, cache: &CacheName, request: CreateCacheRequest) -> Result<()> {
        let endpoint = self
            .endpoint
            .join("_api/v1/cache-config/")?
            .join(cache.as_str())?;

        let res = self.client.post(endpoint).json(&request).send().await?;

        if res.status().is_success() {
            Ok(())
        } else {
            let api_error = ApiError::try_from_response(res).await?;
            Err(api_error.into())
        }
    }

    /// Configures a cache.
    pub async fn configure_cache(&self, cache: &CacheName, config: &CacheConfig) -> Result<()> {
        let endpoint = self
            .endpoint
            .join("_api/v1/cache-config/")?
            .join(cache.as_str())?;

        let res = self.client.patch(endpoint).json(&config).send().await?;

        if res.status().is_success() {
            Ok(())
        } else {
            let api_error = ApiError::try_from_response(res).await?;
            Err(api_error.into())
        }
    }

    /// Destroys a cache.
    pub async fn destroy_cache(&self, cache: &CacheName) -> Result<()> {
        let endpoint = self
            .endpoint
            .join("_api/v1/cache-config/")?
            .join(cache.as_str())?;

        let res = self.client.delete(endpoint).send().await?;

        if res.status().is_success() {
            Ok(())
        } else {
            let api_error = ApiError::try_from_response(res).await?;
            Err(api_error.into())
        }
    }

    /// Returns paths missing from a cache.
    pub async fn get_missing_paths(
        &self,
        cache: &CacheName,
        store_path_hashes: Vec<StorePathHash>,
    ) -> Result<GetMissingPathsResponse> {
        let endpoint = self.endpoint.join("_api/v1/get-missing-paths")?;
        let payload = GetMissingPathsRequest {
            cache: cache.to_owned(),
            store_path_hashes,
        };

        let res = self.client.post(endpoint).json(&payload).send().await?;

        if res.status().is_success() {
            let cache_config = res.json().await?;
            Ok(cache_config)
        } else {
            let api_error = ApiError::try_from_response(res).await?;
            Err(api_error.into())
        }
    }

    /// Returns chunks missing from a cache.
    pub async fn get_missing_chunks(
        &self,
        cache: &CacheName,
        chunk_hashes: Vec<Hash>,
    ) -> Result<GetMissingChunksResponse> {
        let endpoint = self.endpoint.join("_api/v1/get-missing-chunks")?;
        let payload = GetMissingChunksRequest {
            cache: cache.to_owned(),
            chunk_hashes,
        };

        let res = self.client.post(endpoint).json(&payload).send().await?;

        if res.status().is_success() {
            let response = res.json().await?;
            Ok(response)
        } else {
            let api_error = ApiError::try_from_response(res).await?;
            Err(api_error.into())
        }
    }

    /// Uploads a path.
    ///
    /// `body_size` is the number of bytes the request body will actually
    /// contain. This is `nar_info.nar_size` for a normal (non-negotiated)
    /// upload, but for a negotiated (chunk-manifest) upload the body only
    /// contains the manifest's `inline` entries, which is smaller than the
    /// full NAR.
    pub(crate) fn prepare_upload_path(
        &self,
        nar_info: UploadPathNarInfo,
        force_preamble: bool,
        body_size: usize,
    ) -> Result<UploadPathRequest> {
        let endpoint = self.endpoint.join("_api/v1/upload-path")?;
        let upload_info_json = serde_json::to_string(&nar_info)?;
        let metadata = upload_path_metadata(upload_info_json, force_preamble)?;
        let content_length = upload_content_length(&metadata, body_size)?;

        Ok(UploadPathRequest {
            endpoint,
            metadata,
            content_length,
        })
    }

    /// Performs one upload-path HTTP attempt. The caller must create a fresh
    /// NAR stream for each invocation.
    pub(crate) async fn upload_path_attempt<S>(
        &self,
        request: &UploadPathRequest,
        stream: S,
    ) -> std::result::Result<Option<UploadPathResult>, UploadAttemptError>
    where
        S: TryStream<Ok = Bytes> + Send + Sync + 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>> + Send + Sync,
    {
        let mut req = self
            .client
            .put(request.endpoint.clone())
            .header(CONTENT_LENGTH, request.content_length.to_string());

        req = match &request.metadata {
            UploadPathMetadata::Preamble(preamble) => {
                let preamble_len = preamble.len();
                let preamble_stream = stream::once(future::ok(preamble.clone()));
                let chained = preamble_stream.chain(stream.into_stream());
                req.header(ATTIC_NAR_INFO_PREAMBLE_SIZE, preamble_len)
                    .body(Body::wrap_stream(chained))
            }
            UploadPathMetadata::Header(nar_info) => req
                .header(ATTIC_NAR_INFO, nar_info.clone())
                .body(Body::wrap_stream(stream)),
        };

        let res = req
            .send()
            .await
            .map_err(|error| UploadAttemptError::Transport(error.into()))?;

        if res.status().is_success() {
            match res.json().await {
                Ok(r) => Ok(Some(r)),
                Err(_) => Ok(None),
            }
        } else {
            let api_error = ApiError::try_from_response(res)
                .await
                .map_err(|error| UploadAttemptError::Transport(error.into()))?;
            Err(UploadAttemptError::Http(api_error))
        }
    }

    /// Retries only transient upload failures. `make_attempt` must create a
    /// fresh request body each time it is called.
    pub(crate) async fn upload_path_with_retry<T, Attempt, AttemptFuture>(
        &self,
        make_attempt: Attempt,
    ) -> std::result::Result<UploadAttempt<T>, UploadAttemptError>
    where
        Attempt: FnMut(usize) -> AttemptFuture,
        AttemptFuture: Future<Output = std::result::Result<T, UploadAttemptError>>,
    {
        retry_upload_attempts_with(make_attempt, time::sleep).await
    }
}

impl StdError for ApiError {}

impl StdError for UploadAttemptError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Transport(error) => Some(error.as_ref()),
            Self::Http(error) => Some(error),
        }
    }
}

impl fmt::Display for UploadAttemptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(f),
            Self::Http(error) => error.fmt(f),
        }
    }
}

impl ApiError {
    async fn try_from_response(response: Response) -> Result<Self> {
        let status = response.status();
        let retry_after = retry_after(response.headers());
        let text = response.text().await?;
        match serde_json::from_str(&text) {
            Ok(error) => Ok(Self::Structured {
                error,
                status,
                retry_after,
            }),
            Err(_) => Ok(Self::Unstructured {
                status,
                message: text,
                retry_after,
            }),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Structured { status, .. } | Self::Unstructured { status, .. } => *status,
        }
    }

    fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Structured { retry_after, .. } | Self::Unstructured { retry_after, .. } => {
                *retry_after
            }
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Structured { error, .. } => error.fmt(f),
            Self::Unstructured {
                status, message, ..
            } => write!(f, "HTTP {status}: {message}"),
        }
    }
}

impl fmt::Display for StructuredApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error, self.message)
    }
}

fn build_http_client(token: Option<&str>) -> HttpClient {
    let mut headers = HeaderMap::new();

    headers.insert(USER_AGENT, HeaderValue::from_str(ATTIC_USER_AGENT).unwrap());

    if let Some(token) = token {
        let auth_header = HeaderValue::from_str(&format!("bearer {}", token)).unwrap();
        headers.insert(AUTHORIZATION, auth_header);
    }

    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}

fn upload_path_metadata(
    upload_info_json: String,
    force_preamble: bool,
) -> Result<UploadPathMetadata> {
    if force_preamble || upload_info_json.len() >= NAR_INFO_PREAMBLE_THRESHOLD {
        Ok(UploadPathMetadata::Preamble(Bytes::from(upload_info_json)))
    } else {
        Ok(UploadPathMetadata::Header(HeaderValue::from_str(
            &upload_info_json,
        )?))
    }
}

fn upload_content_length(metadata: &UploadPathMetadata, nar_size: usize) -> Result<u64> {
    let nar_size = u64::try_from(nar_size).map_err(|_| anyhow!("NAR size exceeds u64"))?;
    match metadata {
        UploadPathMetadata::Header(_) => Ok(nar_size),
        UploadPathMetadata::Preamble(preamble) => {
            let preamble_len = u64::try_from(preamble.len())
                .map_err(|_| anyhow!("NAR info preamble exceeds u64"))?;
            preamble_len
                .checked_add(nar_size)
                .ok_or_else(|| anyhow!("upload Content-Length overflow"))
        }
    }
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    let date = httpdate::parse_http_date(value).ok()?;
    Some(date.duration_since(SystemTime::now()).unwrap_or_default())
}

fn retryable_upload_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 499 | 500 | 502 | 503 | 504)
}

fn retry_delay(error: &UploadAttemptError, failed_attempt: usize) -> Option<Duration> {
    match error {
        UploadAttemptError::Transport(_) => Some(full_jitter_backoff(failed_attempt)),
        UploadAttemptError::Http(error) if retryable_upload_status(error.status()) => {
            Some(error.retry_after().map_or_else(
                || full_jitter_backoff(failed_attempt),
                |delay| delay.min(UPLOAD_BACKOFF_CAP),
            ))
        }
        UploadAttemptError::Http(_) => None,
    }
}

fn full_jitter_backoff(failed_attempt: usize) -> Duration {
    full_jitter_backoff_with(failed_attempt, |cap| {
        Duration::from_millis(rand::rng().random_range(0..=cap.as_millis() as u64))
    })
}

fn full_jitter_backoff_with(
    failed_attempt: usize,
    random: impl FnOnce(Duration) -> Duration,
) -> Duration {
    let exponent = u32::try_from(failed_attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let cap = UPLOAD_BACKOFF_BASE
        .checked_mul(2_u32.saturating_pow(exponent))
        .unwrap_or(UPLOAD_BACKOFF_CAP)
        .min(UPLOAD_BACKOFF_CAP);
    random(cap).min(cap)
}

async fn retry_upload_attempts_with<T, Attempt, AttemptFuture, Sleep, SleepFuture>(
    mut make_attempt: Attempt,
    mut sleep: Sleep,
) -> std::result::Result<UploadAttempt<T>, UploadAttemptError>
where
    Attempt: FnMut(usize) -> AttemptFuture,
    AttemptFuture: Future<Output = std::result::Result<T, UploadAttemptError>>,
    Sleep: FnMut(Duration) -> SleepFuture,
    SleepFuture: Future<Output = ()>,
{
    for attempt in 1..=UPLOAD_MAX_ATTEMPTS {
        match make_attempt(attempt).await {
            Ok(value) => {
                if attempt > 1 {
                    tracing::info!(attempt, "Upload succeeded after retry");
                }
                return Ok(UploadAttempt {
                    value,
                    attempts: attempt,
                });
            }
            Err(error) => {
                let Some(delay) = retry_delay(&error, attempt) else {
                    return Err(error);
                };
                if attempt == UPLOAD_MAX_ATTEMPTS {
                    return Err(error);
                }

                tracing::warn!(attempt, delay_ms = delay.as_millis(), error = %error, "Retrying upload after transient failure");
                sleep(delay).await;
            }
        }
    }

    unreachable!("the retry loop always returns")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    fn http_error(status: u16, retry_after: Option<Duration>) -> UploadAttemptError {
        UploadAttemptError::Http(ApiError::Unstructured {
            status: StatusCode::from_u16(status).unwrap(),
            message: String::new(),
            retry_after,
        })
    }

    #[test]
    fn metadata_mode_and_content_length_are_wire_compatible() {
        let header = upload_path_metadata("{}".to_owned(), false).unwrap();
        assert!(matches!(header, UploadPathMetadata::Header(_)));
        assert_eq!(upload_content_length(&header, 42).unwrap(), 42);

        let preamble = upload_path_metadata("{}".to_owned(), true).unwrap();
        assert!(matches!(preamble, UploadPathMetadata::Preamble(_)));
        assert_eq!(upload_content_length(&preamble, 42).unwrap(), 44);
    }

    #[test]
    fn content_length_overflow_is_rejected() {
        let metadata = UploadPathMetadata::Preamble(Bytes::from_static(b"x"));
        if usize::BITS == 64 {
            let error = upload_content_length(&metadata, usize::MAX).unwrap_err();
            assert_eq!(error.to_string(), "upload Content-Length overflow");
        } else {
            assert_eq!(
                upload_content_length(&metadata, usize::MAX).unwrap(),
                u64::from(usize::MAX as u32) + 1
            );
        }
    }

    #[test]
    fn retry_classification_is_limited_to_transient_statuses() {
        for status in [408, 429, 499, 500, 502, 503, 504] {
            assert!(retryable_upload_status(
                StatusCode::from_u16(status).unwrap()
            ));
        }
        for status in [400, 401, 403, 404, 409, 413, 422, 501] {
            assert!(!retryable_upload_status(
                StatusCode::from_u16(status).unwrap()
            ));
        }
    }

    #[test]
    fn full_jitter_backoff_is_capped() {
        assert_eq!(
            full_jitter_backoff_with(1, |cap| cap),
            Duration::from_millis(500)
        );
        assert_eq!(
            full_jitter_backoff_with(2, |cap| cap),
            Duration::from_secs(1)
        );
        assert_eq!(full_jitter_backoff_with(10, |cap| cap), UPLOAD_BACKOFF_CAP);
        assert_eq!(
            retry_delay(&http_error(503, Some(Duration::from_secs(10))), 1),
            Some(UPLOAD_BACKOFF_CAP)
        );

        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(2)));
    }

    #[tokio::test]
    async fn synthetic_transport_error_reinvokes_attempt_factory() {
        // This exercises retry-loop control flow with a synthetic error. It
        // does not create a network disconnect or reconstruct a NAR body.
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let slept = Arc::new(Mutex::new(Vec::new()));
        let outcome = retry_upload_attempts_with(
            {
                let attempts = attempts.clone();
                move |attempt| {
                    attempts.lock().unwrap().push(attempt);
                    std::future::ready(if attempt == 1 {
                        Err(UploadAttemptError::Transport(anyhow!("connection closed")))
                    } else {
                        Ok("retried")
                    })
                }
            },
            {
                let slept = slept.clone();
                move |delay| {
                    slept.lock().unwrap().push(delay);
                    std::future::ready(())
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(outcome.value, "retried");
        assert_eq!(outcome.attempts, 2);
        assert_eq!(*attempts.lock().unwrap(), [1, 2]);
        assert_eq!(slept.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn synthetic_transient_status_retries_after_unknown_outcome() {
        // This is a synthetic transient HTTP status. It does not prove that a
        // server committed an upload before a response was lost.
        let attempts = Arc::new(Mutex::new(0));
        let outcome = retry_upload_attempts_with(
            {
                let attempts = attempts.clone();
                move |_| {
                    let mut attempts = attempts.lock().unwrap();
                    *attempts += 1;
                    std::future::ready(if *attempts == 1 {
                        Err(http_error(503, None))
                    } else {
                        Ok(())
                    })
                }
            },
            |_| std::future::ready(()),
        )
        .await
        .unwrap();

        assert_eq!(outcome.attempts, 2);
    }

    #[tokio::test]
    async fn retries_stop_after_three_attempts_and_not_for_permanent_errors() {
        let attempts = Arc::new(Mutex::new(0));
        let error = retry_upload_attempts_with(
            {
                let attempts = attempts.clone();
                move |_| {
                    *attempts.lock().unwrap() += 1;
                    std::future::ready(Err::<(), _>(http_error(503, None)))
                }
            },
            |_| std::future::ready(()),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, UploadAttemptError::Http(_)));
        assert_eq!(*attempts.lock().unwrap(), UPLOAD_MAX_ATTEMPTS);

        let permanent_attempts = Arc::new(Mutex::new(0));
        let _ = retry_upload_attempts_with(
            {
                let permanent_attempts = permanent_attempts.clone();
                move |_| {
                    *permanent_attempts.lock().unwrap() += 1;
                    std::future::ready(Err::<(), _>(http_error(401, None)))
                }
            },
            |_| std::future::ready(()),
        )
        .await;
        assert_eq!(*permanent_attempts.lock().unwrap(), 1);
    }
}
