//! OIDC token exchange endpoints.

use axum::{
    Json,
    extract::Extension,
    http::{HeaderValue, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::time::{Duration, Instant};

use crate::{
    CachedOidcKeyset, State,
    access::{OidcToken, RS256PublicKey, Token},
    config::{OidcProviderConfig, OidcProviderMode},
    error::{ErrorKind, ServerError, ServerResult},
};

#[derive(Serialize)]
pub struct ProvidersResponse {
    providers: Vec<ProviderResponse>,
}

#[derive(Serialize)]
pub struct ProviderResponse {
    name: String,
    display_name: String,
    mode: OidcProviderMode,
    issuer: String,
    audience: String,
    authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
    scopes: Vec<String>,
}

#[derive(Deserialize)]
pub struct ExchangeRequest {
    provider: String,
    id_token: String,
}

#[derive(Serialize)]
pub struct ExchangeResponse {
    access_token: String,
    token_type: &'static str,
    expires_in: u64,
}

#[derive(Deserialize)]
struct JwtHeader {
    alg: String,
    kid: Option<String>,
}

pub async fn providers(Extension(state): Extension<State>) -> Json<ProvidersResponse> {
    Json(ProvidersResponse {
        providers: state
            .config
            .oidc
            .providers
            .iter()
            .map(|provider| ProviderResponse {
                name: provider.name.clone(),
                display_name: provider
                    .display_name
                    .clone()
                    .unwrap_or_else(|| provider.name.clone()),
                mode: provider.mode,
                issuer: provider.issuer.clone(),
                audience: provider.audience.clone(),
                authorization_endpoint: provider.authorization_endpoint.clone(),
                token_endpoint: provider.token_endpoint.clone(),
                scopes: provider.scopes.clone(),
            })
            .collect(),
    })
}

pub async fn exchange(
    Extension(state): Extension<State>,
    Json(request): Json<ExchangeRequest>,
) -> ServerResult<Response> {
    let provider = state
        .config
        .oidc
        .providers
        .iter()
        .find(|provider| provider.name == request.provider)
        .ok_or_else(|| ServerError::from(ErrorKind::NotFound))?;

    let token = verify_token(&state, provider, &request.id_token).await?;
    let permissions = provider.permissions_for_claims(token.claims());
    if permissions
        .values()
        .all(|permission| !permission.can_discover())
    {
        tracing::warn!(provider = %provider.name, subject = ?token.sub(), "OIDC identity matched no Attic permissions");
        return Err(ErrorKind::Forbidden.into());
    }

    let subject = token
        .sub()
        .ok_or_else(|| ServerError::from(ErrorKind::Unauthorized))?;
    let validity =
        ChronoDuration::from_std(provider.token_validity).map_err(ServerError::request_error)?;
    let expires_at = Utc::now() + validity;
    let mut attic_token = Token::new(format!("oidc:{}:{subject}", provider.name), &expires_at);
    for (cache, permission) in permissions {
        *attic_token.get_or_insert_permission_mut(cache) = permission;
    }

    let signature_type = state.config.jwt.signing_config.clone().into();
    let access_token = attic_token
        .encode(
            &signature_type,
            &state.config.jwt.token_bound_issuer,
            &state.config.jwt.token_bound_audiences,
        )
        .map_err(|error| {
            ServerError::from(ErrorKind::RequestError(anyhow::anyhow!(error.to_string())))
        })?;

    tracing::info!(provider = %provider.name, subject, "issued OIDC Attic token");
    let response = ExchangeResponse {
        access_token,
        token_type: "Bearer",
        expires_in: provider.token_validity.as_secs(),
    };
    Ok((
        [(CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(response),
    )
        .into_response())
}

async fn verify_token(
    state: &State,
    provider: &OidcProviderConfig,
    token: &str,
) -> ServerResult<OidcToken> {
    let header = parse_header(token)?;
    if header.alg != "RS256" {
        tracing::debug!(provider = %provider.name, algorithm = %header.alg, "rejected OIDC token algorithm");
        return Err(ErrorKind::Unauthorized.into());
    }
    let kid = header
        .kid
        .ok_or_else(|| ServerError::from(ErrorKind::Unauthorized))?;

    let keys = keyset_for(state, provider, &kid).await?;
    let jwk = keys
        .iter()
        .find(|key| key.get("kid").and_then(JsonValue::as_str) == Some(kid.as_str()))
        .filter(|key| {
            key.get("kty").and_then(JsonValue::as_str) == Some("RSA")
                && key
                    .get("alg")
                    .and_then(JsonValue::as_str)
                    .is_none_or(|alg| alg == "RS256")
        })
        .ok_or_else(|| ServerError::from(ErrorKind::Unauthorized))?;
    let modulus = URL_SAFE_NO_PAD
        .decode(
            jwk.get("n")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| ServerError::from(ErrorKind::Unauthorized))?,
        )
        .map_err(|_| ServerError::from(ErrorKind::Unauthorized))?;
    let exponent = URL_SAFE_NO_PAD
        .decode(
            jwk.get("e")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| ServerError::from(ErrorKind::Unauthorized))?,
        )
        .map_err(|_| ServerError::from(ErrorKind::Unauthorized))?;
    let public_key = RS256PublicKey::from_components(&modulus, &exponent)
        .map_err(|_| ServerError::from(ErrorKind::Unauthorized))?;

    OidcToken::from_rs256_jwt(token, &public_key, &provider.issuer, &provider.audience).map_err(
        |error| {
            tracing::debug!(provider = %provider.name, error = %error, "rejected OIDC ID token");
            ServerError::from(ErrorKind::Unauthorized)
        },
    )
}

async fn keyset_for(
    state: &State,
    provider: &OidcProviderConfig,
    kid: &str,
) -> ServerResult<Vec<JsonValue>> {
    let now = Instant::now();
    let cached = {
        let cache = state.oidc_keysets.lock().await;
        cache
            .get(&provider.name)
            .map(|cached| (cached.refresh_at > now, cached.keys.clone()))
    };
    if let Some((true, keys)) = cached.as_ref()
        && contains_key(keys, kid)
    {
        return Ok(keys.clone());
    }

    match fetch_keyset(provider).await {
        Ok(keys) => {
            state.oidc_keysets.lock().await.insert(
                provider.name.clone(),
                CachedOidcKeyset {
                    refresh_at: now + Duration::from_secs(60 * 60),
                    keys: keys.clone(),
                },
            );
            Ok(keys)
        }
        Err(_)
            if cached
                .as_ref()
                .is_some_and(|(_, keys)| contains_key(keys, kid)) =>
        {
            Ok(cached.unwrap().1)
        }
        Err(error) => Err(error),
    }
}

fn contains_key(keys: &[JsonValue], kid: &str) -> bool {
    keys.iter()
        .any(|key| key.get("kid").and_then(JsonValue::as_str) == Some(kid))
}

async fn fetch_keyset(provider: &OidcProviderConfig) -> ServerResult<Vec<JsonValue>> {
    let value = reqwest::get(&provider.jwks_url)
        .await
        .map_err(|_| ServerError::from(ErrorKind::ServiceUnavailable))?
        .error_for_status()
        .map_err(|_| ServerError::from(ErrorKind::ServiceUnavailable))?
        .json::<JsonValue>()
        .await
        .map_err(|_| ServerError::from(ErrorKind::ServiceUnavailable))?;
    value
        .get("keys")
        .and_then(JsonValue::as_array)
        .cloned()
        .ok_or_else(|| ServerError::from(ErrorKind::ServiceUnavailable))
}

fn parse_header(token: &str) -> ServerResult<JwtHeader> {
    let encoded = token
        .split_once('.')
        .map(|(header, _)| header)
        .ok_or_else(|| ServerError::from(ErrorKind::Unauthorized))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ServerError::from(ErrorKind::Unauthorized))?;
    serde_json::from_slice(&bytes).map_err(|_| ServerError::from(ErrorKind::Unauthorized))
}
