//! OIDC login flows used by `attic login --oidc`.

use std::env;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::distr::{Alphanumeric, SampleString};
use reqwest::{Client, Url, header::AUTHORIZATION};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};

use crate::api::{ApiClient, OidcProvider, OidcProviderMode};

/// Logs into an OIDC provider advertised by an Attic server.
pub async fn login(endpoint: &str, provider_name: &str) -> Result<String> {
    let provider = ApiClient::oidc_providers(endpoint)
        .await?
        .into_iter()
        .find(|provider| provider.name == provider_name)
        .ok_or_else(|| anyhow!("The Attic server has no OIDC provider named {provider_name:?}"))?;

    let id_token = match provider.mode {
        OidcProviderMode::AuthorizationCodePkce => login_pkce(&provider).await?,
        OidcProviderMode::GithubActions => github_actions_token(&provider).await?,
    };

    ApiClient::exchange_oidc_token(endpoint, &provider.name, &id_token).await
}

async fn github_actions_token(provider: &OidcProvider) -> Result<String> {
    let request_url = env::var("ACTIONS_ID_TOKEN_REQUEST_URL").context(
        "GitHub Actions OIDC is unavailable: set permissions.id-token to write and run this command inside a GitHub Actions job",
    )?;
    let request_token = env::var("ACTIONS_ID_TOKEN_REQUEST_TOKEN").context(
        "GitHub Actions OIDC request token is unavailable: set permissions.id-token to write",
    )?;
    let mut url = Url::parse(&request_url)?;
    url.query_pairs_mut()
        .append_pair("audience", &provider.audience);

    #[derive(Deserialize)]
    struct GithubResponse {
        value: String,
    }

    let response = Client::new()
        .get(url)
        .header(AUTHORIZATION, format!("Bearer {request_token}"))
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json::<GithubResponse>().await?.value)
}

async fn login_pkce(provider: &OidcProvider) -> Result<String> {
    let authorization_endpoint = provider.authorization_endpoint.as_deref().ok_or_else(|| {
        anyhow!(
            "OIDC provider {:?} did not provide an authorization endpoint",
            provider.name
        )
    })?;
    let token_endpoint = provider.token_endpoint.as_deref().ok_or_else(|| {
        anyhow!(
            "OIDC provider {:?} did not provide a token endpoint",
            provider.name
        )
    })?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let redirect_uri = format!("http://{}/callback", listener.local_addr()?);
    let state = random_string(32);
    let nonce = random_string(32);
    let verifier = random_string(64);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let scope = provider.scopes.join(" ");

    let mut authorization_url = Url::parse(authorization_endpoint)?;
    authorization_url.query_pairs_mut().extend_pairs([
        ("response_type", "code"),
        ("client_id", provider.audience.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("scope", scope.as_str()),
        ("state", state.as_str()),
        ("nonce", nonce.as_str()),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
    ]);

    eprintln!("Opening {} in your browser…", provider.display_name);
    if let Err(error) = open::that(authorization_url.as_str()) {
        eprintln!(
            "Could not open a browser ({error}). Open this URL manually:\n{authorization_url}"
        );
    }

    let code = receive_callback(&listener, &state).await?;
    #[derive(Deserialize)]
    struct TokenResponse {
        id_token: String,
    }
    let response = Client::new()
        .post(token_endpoint)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", provider.audience.as_str()),
            ("code", code.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("code_verifier", verifier.as_str()),
        ])
        .send()
        .await?
        .error_for_status()?;
    let id_token = response.json::<TokenResponse>().await?.id_token;
    validate_nonce(&id_token, &nonce)?;
    Ok(id_token)
}

async fn receive_callback(listener: &TcpListener, expected_state: &str) -> Result<String> {
    let (mut stream, _) = timeout(Duration::from_secs(300), listener.accept())
        .await
        .context("Timed out waiting for the browser login callback")??;
    let mut request = vec![0; 8192];
    let read = stream.read(&mut request).await?;
    let request =
        std::str::from_utf8(&request[..read]).context("OIDC callback was not valid UTF-8")?;
    let target = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow!("OIDC callback request was malformed"))?;
    let url = Url::parse(&format!("http://localhost{target}"))?;
    let values: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    let body = if values.contains_key("error") {
        "Login failed. You can close this window."
    } else {
        "Login complete. You can close this window."
    };
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await?;

    if let Some(error) = values.get("error") {
        bail!("OIDC provider returned {error}");
    }
    if values.get("state").map(String::as_str) != Some(expected_state) {
        bail!("OIDC callback state did not match the login request");
    }
    values
        .get("code")
        .cloned()
        .ok_or_else(|| anyhow!("OIDC callback did not include an authorization code"))
}

fn validate_nonce(id_token: &str, expected_nonce: &str) -> Result<()> {
    let payload = id_token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow!("OIDC provider returned a malformed ID token"))?;
    let bytes = URL_SAFE_NO_PAD.decode(payload)?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes)?;
    if claims.get("nonce").and_then(serde_json::Value::as_str) != Some(expected_nonce) {
        bail!("OIDC ID token nonce did not match the login request");
    }
    Ok(())
}

fn random_string(len: usize) -> String {
    Alphanumeric.sample_string(&mut rand::rng(), len)
}

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

    use super::validate_nonce;

    #[test]
    fn validates_nonce_in_id_token_payload() {
        let payload = URL_SAFE_NO_PAD.encode(r#"{"nonce":"expected"}"#);
        let token = format!("header.{payload}.signature");
        assert!(validate_nonce(&token, "expected").is_ok());
        assert!(validate_nonce(&token, "wrong").is_err());
    }
}
