use crate::index::{exact_fallback_name, Index, SearchParams};
use crate::parser::parse_payload;
use crate::vector::vectorize;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::env;
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::runtime::Builder;

const MAX_REQUEST_BYTES: usize = 32 * 1024;
const BODY_APPROVED_0: &[u8] = b"{\"approved\":true,\"fraud_score\":0.0}";
const BODY_APPROVED_02: &[u8] = b"{\"approved\":true,\"fraud_score\":0.2}";
const BODY_APPROVED_04: &[u8] = b"{\"approved\":true,\"fraud_score\":0.4}";
const BODY_REJECTED_06: &[u8] = b"{\"approved\":false,\"fraud_score\":0.6}";
const BODY_REJECTED_08: &[u8] = b"{\"approved\":false,\"fraud_score\":0.8}";
const BODY_REJECTED_1: &[u8] = b"{\"approved\":false,\"fraud_score\":1.0}";

type HttpBody = Full<Bytes>;

pub fn serve() -> Result<(), String> {
    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let index_path =
        env::var("INDEX_PATH").unwrap_or_else(|_| "/app/data/references.idx".to_string());
    let workers = env_usize("WORKERS", 4).max(1);
    let keep_alive_requests = env_usize("KEEP_ALIVE_REQUESTS", 256).max(1);
    let keep_alive = keep_alive_requests > 1;
    let unix_socket_path = parse_unix_socket_path(&bind_addr).map(str::to_owned);
    let params = Arc::new(SearchParams::from_env());
    let index = Arc::new(Index::open(&index_path)?);
    if env_bool("PREFETCH_INDEX", true) {
        let checksum = index.prefault();
        eprintln!("prefetched index pages, checksum={checksum}");
    }
    let load = Arc::new(AtomicUsize::new(0));

    eprintln!(
        "serving on {bind_addr}, index={index_path}, workers={workers}, keep_alive_requests={keep_alive_requests}, accept=hyper-http1, early_candidates={}, min_candidates={}, max_candidates={}, profile_fastpath={}, profile_min_count={}, exact_fallback={}, risky_fallback_refs={}, overload_min_candidates={}, overload_max_candidates={}, overload_threshold={}, overload_fast_only={}, search_fallback_last_distance={}, flat={}, fast_path={}, fast_only={}",
        params.early_candidates,
        params.min_candidates,
        params.max_candidates,
        params.profile_fast_path,
        params.profile_min_count,
        exact_fallback_name(params.exact_fallback),
        index.risky_fallback_count(),
        params.overload_min_candidates,
        params.overload_max_candidates,
        params.overload_threshold,
        params.overload_fast_only,
        params.search_fallback_last_distance,
        params.flat,
        params.fast_path,
        params.fast_only
    );

    let runtime = Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {e}"))?;

    runtime.block_on(async move {
        if let Some(unix_socket_path) = unix_socket_path {
            #[cfg(unix)]
            {
                return serve_unix(
                    &unix_socket_path,
                    index,
                    params,
                    load,
                    keep_alive,
                )
                .await;
            }
            #[cfg(not(unix))]
            {
                let _ = unix_socket_path;
                return Err("unix sockets are only supported on unix targets".to_string());
            }
        }

        serve_tcp(&bind_addr, index, params, load, keep_alive).await
    })
}

async fn serve_tcp(
    bind_addr: &str,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
    keep_alive: bool,
) -> Result<(), String> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept error: {e}"))?;
        let _ = stream.set_nodelay(true);
        spawn_connection(TokioIo::new(stream), &index, &params, &load, keep_alive);
    }
}

#[cfg(unix)]
async fn serve_unix(
    socket_path: &str,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
    keep_alive: bool,
) -> Result<(), String> {
    if let Some(parent) = Path::new(socket_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create socket dir {}: {e}", parent.display()))?;
    }
    try_delete_unix_socket(socket_path);

    let listener = UnixListener::bind(socket_path)
        .map_err(|e| format!("failed to bind unix socket {socket_path}: {e}"))?;
    set_unix_socket_permissions(socket_path);

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept error: {e}"))?;
        spawn_connection(TokioIo::new(stream), &index, &params, &load, keep_alive);
    }
}

fn spawn_connection<I>(
    io: I,
    index: &Arc<Index>,
    params: &Arc<SearchParams>,
    load: &Arc<AtomicUsize>,
    keep_alive: bool,
) where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let index = Arc::clone(index);
    let params = Arc::clone(params);
    let load = Arc::clone(load);

    tokio::spawn(async move {
        let service = service_fn(move |request| {
            handle_request(
                request,
                Arc::clone(&index),
                Arc::clone(&params),
                Arc::clone(&load),
            )
        });

        let mut builder = http1::Builder::new();
        builder.keep_alive(keep_alive);

        if let Err(err) = builder.serve_connection(io, service).await {
            eprintln!("connection error: {err}");
        }
    });
}

async fn handle_request(
    request: Request<Incoming>,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
) -> Result<Response<HttpBody>, Infallible> {
    if request.method() == Method::GET && request.uri().path() == "/ready" {
        return Ok(text_response(StatusCode::OK, b"OK"));
    }

    if request.method() != Method::POST || request.uri().path() != "/fraud-score" {
        return Ok(text_response(StatusCode::NOT_FOUND, b"not found"));
    }

    let body = match request.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Ok(json_response(BODY_APPROVED_0)),
    };
    if body.len() > MAX_REQUEST_BYTES {
        return Ok(json_response(BODY_APPROVED_0));
    }

    let body = std::str::from_utf8(&body).unwrap_or("");

    let response = match parse_payload(body) {
        Ok(payload) => {
            let _guard = InFlightGuard::new(&load);
            let query = vectorize(&payload);
            let classify_params = params.for_load(load.load(Ordering::Relaxed));
            let (approved, score) = index.classify(&query, &classify_params);
            fraud_response_body(approved, score)
        }
        Err(_) => BODY_APPROVED_0,
    };

    Ok(json_response(response))
}

struct InFlightGuard<'a> {
    load: &'a AtomicUsize,
}

impl<'a> InFlightGuard<'a> {
    fn new(load: &'a AtomicUsize) -> Self {
        load.fetch_add(1, Ordering::Relaxed);
        Self { load }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.load.fetch_sub(1, Ordering::Relaxed);
    }
}

fn fraud_response_body(approved: bool, score: f32) -> &'static [u8] {
    if approved {
        if score < 0.1 {
            BODY_APPROVED_0
        } else if score < 0.3 {
            BODY_APPROVED_02
        } else {
            BODY_APPROVED_04
        }
    } else if score < 0.7 {
        BODY_REJECTED_06
    } else if score < 0.9 {
        BODY_REJECTED_08
    } else {
        BODY_REJECTED_1
    }
}

fn json_response(body: &'static [u8]) -> Response<HttpBody> {
    let mut response = Response::new(Full::new(Bytes::from_static(body)));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn text_response(status: StatusCode, body: &'static [u8]) -> Response<HttpBody> {
    let mut response = Response::new(Full::new(Bytes::from_static(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn parse_unix_socket_path(bind_addr: &str) -> Option<&str> {
    bind_addr.strip_prefix("unix:")
}

#[cfg(unix)]
fn try_delete_unix_socket(path: &str) {
    let _ = fs::remove_file(path);
}

#[cfg(unix)]
fn set_unix_socket_permissions(path: &str) {
    let permissions = fs::Permissions::from_mode(0o666);
    let _ = fs::set_permissions(path, permissions);
}
