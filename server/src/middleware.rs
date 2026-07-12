use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::anyhow;
use axum::{
    extract::{Extension, Request},
    http::{
        HeaderMap, HeaderValue,
        header::{HOST, HeaderName},
    },
    middleware::Next,
    response::Response,
};
use tracing::Instrument;
use uuid::Uuid;

use super::{AuthState, RequestId, RequestState, RequestStateInner, State};
use crate::error::{ErrorKind, ServerResult};
use attic::api::binary_cache::ATTIC_CACHE_VISIBILITY;

const REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

fn host_header(req: &Request) -> ServerResult<String> {
    Ok(req
        .headers()
        .get(HOST)
        .ok_or_else(|| ErrorKind::RequestError(anyhow!("Missing Host header")))?
        .to_str()
        .map(str::to_owned)
        .map_err(|_| ErrorKind::RequestError(anyhow!("Invalid Host header")))?)
}

fn request_id(headers: &HeaderMap) -> Uuid {
    headers
        .get(&REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or_else(Uuid::new_v4)
}

/// Selects a safe request ID before the rest of the application stack runs.
pub async fn assign_request_id(mut req: Request, next: Next) -> Response {
    let request_id = RequestId(request_id(req.headers()));
    req.extensions_mut().insert(request_id);
    let span = tracing::info_span!("request", request_id = %request_id.0);
    set_request_id_header(next.run(req).instrument(span).await, request_id.0)
}

/// Initializes per-request state.
pub async fn init_request_state(
    Extension(state): Extension<State>,
    Extension(request_id): Extension<RequestId>,
    mut req: Request,
    next: Next,
) -> ServerResult<Response> {
    let host = host_header(&req)?;
    // X-Forwarded-Proto is an untrusted header
    let client_claims_https =
        if let Some(x_forwarded_proto) = req.headers().get("x-forwarded-proto") {
            x_forwarded_proto.as_bytes() == b"https"
        } else {
            false
        };

    let req_state = Arc::new(RequestStateInner {
        request_id: request_id.0,
        auth: AuthState::new(),
        api_endpoint: state.config.api_endpoint.to_owned(),
        substituter_endpoint: state.config.substituter_endpoint.to_owned(),
        host,
        client_claims_https,
        public_cache: AtomicBool::new(false),
    });

    req.extensions_mut().insert(req_state);
    Ok(next.run(req).await)
}

fn set_request_id_header(mut response: Response, request_id: Uuid) -> Response {
    response.headers_mut().insert(
        REQUEST_ID,
        HeaderValue::from_str(&request_id.to_string()).expect("UUID is a valid header value"),
    );
    response
}

/// Restricts valid Host headers.
///
/// We also require that all request have a Host header in
/// the first place.
pub async fn restrict_host(
    Extension(state): Extension<State>,
    req: Request,
    next: Next,
) -> ServerResult<Response> {
    let host = host_header(&req)?;
    let allowed_hosts = &state.config.allowed_hosts;

    if !allowed_hosts.is_empty() && !allowed_hosts.iter().any(|h| h.as_str() == host) {
        return Err(ErrorKind::RequestError(anyhow!("Bad Host")).into());
    }

    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{HeaderMap, HeaderValue, Request, StatusCode},
        middleware,
        response::Response,
        routing::get,
    };
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::{REQUEST_ID, assign_request_id, request_id, set_request_id_header};

    #[test]
    fn request_id_reuses_valid_uuid_and_rejects_untrusted_values() {
        let expected = "c5a15b09-459f-4d7d-a424-6e5ed655c379";
        let mut headers = HeaderMap::new();
        headers.insert(REQUEST_ID, HeaderValue::from_static(expected));
        assert_eq!(request_id(&headers).to_string(), expected);

        headers.insert(REQUEST_ID, HeaderValue::from_static("not-a-request-id"));
        assert_ne!(request_id(&headers).to_string(), "not-a-request-id");
    }

    #[test]
    fn request_id_is_returned_on_middleware_responses() {
        let request_id = Uuid::parse_str("c5a15b09-459f-4d7d-a424-6e5ed655c379").unwrap();
        let response = set_request_id_header(Response::new(Body::empty()), request_id);
        assert_eq!(
            response
                .headers()
                .get(REQUEST_ID)
                .unwrap()
                .to_str()
                .unwrap(),
            request_id.to_string()
        );
    }

    fn app(status: StatusCode) -> Router {
        Router::new()
            .route("/", get(move || async move { status }))
            .layer(middleware::from_fn(assign_request_id))
    }

    #[tokio::test]
    async fn production_request_id_layer_covers_success_and_handler_errors() {
        let valid = "c5a15b09-459f-4d7d-a424-6e5ed655c379";
        let response = app(StatusCode::OK)
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(REQUEST_ID, valid)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get(REQUEST_ID).unwrap(), valid);

        let generated = app(StatusCode::BAD_REQUEST)
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(REQUEST_ID, "invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(generated.status(), StatusCode::BAD_REQUEST);
        assert!(
            Uuid::parse_str(
                generated
                    .headers()
                    .get(REQUEST_ID)
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn production_request_id_layer_covers_missing_ids_and_inner_rejections() {
        let missing = app(StatusCode::INTERNAL_SERVER_ERROR)
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            Uuid::parse_str(missing.headers().get(REQUEST_ID).unwrap().to_str().unwrap()).is_ok()
        );

        let rejection = Router::new()
            .route("/", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn(
                |_req: Request<Body>, _next: middleware::Next| async { StatusCode::FORBIDDEN },
            ))
            .layer(middleware::from_fn(assign_request_id))
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(rejection.status(), StatusCode::FORBIDDEN);
        assert!(
            Uuid::parse_str(
                rejection
                    .headers()
                    .get(REQUEST_ID)
                    .unwrap()
                    .to_str()
                    .unwrap()
            )
            .is_ok()
        );
    }
}

/// Sets the `X-Attic-Cache-Visibility` header in responses.
pub(crate) async fn set_visibility_header(
    Extension(req_state): Extension<RequestState>,
    req: Request,
    next: Next,
) -> ServerResult<Response> {
    let mut response = next.run(req).await;

    if req_state.public_cache.load(Ordering::Relaxed) {
        response
            .headers_mut()
            .append(ATTIC_CACHE_VISIBILITY, HeaderValue::from_static("public"));
    }

    Ok(response)
}
