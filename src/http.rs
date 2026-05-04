use crate::index::{exact_fallback_name, Index, SearchParams};
use crate::parser::parse_payload;
use crate::vector::vectorize;
use std::env;
#[cfg(unix)]
use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::runtime::Builder;

const MAX_REQUEST_BYTES: usize = 32 * 1024;
const RX_CAP: usize = 64 * 1024;
const MAX_BATCHED_RESPONSES: usize = 16;

const RESP_APPROVED_0: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.0}";
const RESP_APPROVED_02: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.2}";
const RESP_APPROVED_04: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.4}";
const RESP_REJECTED_06: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\n\r\n{\"approved\":false,\"fraud_score\":0.6}";
const RESP_REJECTED_08: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\n\r\n{\"approved\":false,\"fraud_score\":0.8}";
const RESP_REJECTED_1: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 36\r\n\r\n{\"approved\":false,\"fraud_score\":1.0}";
const RESP_READY: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK";
const RESP_NOT_FOUND: &[u8] = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
const RESP_BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

enum ParsedRequest {
    Incomplete,
    Bad,
    Ready { consumed: usize },
    NotFound { consumed: usize },
    Fraud {
        body_start: usize,
        body_end: usize,
        consumed: usize,
    },
}

pub fn serve() -> Result<(), String> {
    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let index_path =
        env::var("INDEX_PATH").unwrap_or_else(|_| "/app/data/references.idx".to_string());
    let workers = env_usize("WORKERS", 4).max(1);
    let keep_alive_requests = env_usize("KEEP_ALIVE_REQUESTS", 256).max(1);
    let unix_socket_path = parse_unix_socket_path(&bind_addr).map(str::to_owned);
    let params = Arc::new(SearchParams::from_env());
    let index = Arc::new(Index::open(&index_path)?);
    if env_bool("PREFETCH_INDEX", true) {
        let checksum = index.prefault();
        eprintln!("prefetched index pages, checksum={checksum}");
    }
    let load = Arc::new(AtomicUsize::new(0));

    eprintln!(
        "serving on {bind_addr}, index={index_path}, workers={workers}, keep_alive_requests={keep_alive_requests}, accept=manual-http1, early_candidates={}, min_candidates={}, max_candidates={}, profile_fastpath={}, profile_min_count={}, exact_fallback={}, risky_fallback_refs={}, overload_min_candidates={}, overload_max_candidates={}, overload_threshold={}, overload_fast_only={}, search_fallback_last_distance={}, flat={}, fast_path={}, fast_only={}",
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
                return serve_unix(&unix_socket_path, index, params, load, keep_alive_requests).await;
            }
            #[cfg(not(unix))]
            {
                let _ = unix_socket_path;
                return Err("unix sockets are only supported on unix targets".to_string());
            }
        }

        serve_tcp(&bind_addr, index, params, load, keep_alive_requests).await
    })
}

async fn serve_tcp(
    bind_addr: &str,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
    keep_alive_requests: usize,
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
        spawn_connection(stream, &index, &params, &load, keep_alive_requests);
    }
}

#[cfg(unix)]
async fn serve_unix(
    socket_path: &str,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
    keep_alive_requests: usize,
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
        spawn_connection(stream, &index, &params, &load, keep_alive_requests);
    }
}

fn spawn_connection<S>(
    stream: S,
    index: &Arc<Index>,
    params: &Arc<SearchParams>,
    load: &Arc<AtomicUsize>,
    keep_alive_requests: usize,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let index = Arc::clone(index);
    let params = Arc::clone(params);
    let load = Arc::clone(load);

    tokio::spawn(async move {
        if let Err(err) = serve_connection(stream, index, params, load, keep_alive_requests).await {
            eprintln!("connection error: {err}");
        }
    });
}

async fn serve_connection<S>(
    mut stream: S,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
    keep_alive_requests: usize,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut rx = vec![0u8; RX_CAP];
    let mut responses: Vec<&'static [u8]> = Vec::with_capacity(MAX_BATCHED_RESPONSES);
    let mut head = 0usize;
    let mut tail = 0usize;
    let mut handled = 0usize;

    loop {
        responses.clear();
        let mut close_after_write = false;

        while head < tail && responses.len() < MAX_BATCHED_RESPONSES {
            match parse_request(&rx[head..tail]) {
                ParsedRequest::Incomplete => break,
                ParsedRequest::Bad => {
                    responses.push(RESP_BAD_REQUEST);
                    close_after_write = true;
                    head = tail;
                    break;
                }
                ParsedRequest::Ready { consumed } => {
                    responses.push(RESP_READY);
                    head += consumed;
                    handled += 1;
                }
                ParsedRequest::NotFound { consumed } => {
                    responses.push(RESP_NOT_FOUND);
                    head += consumed;
                    handled += 1;
                }
                ParsedRequest::Fraud {
                    body_start,
                    body_end,
                    consumed,
                } => {
                    let body = &rx[head + body_start..head + body_end];
                    responses.push(process_fraud(body, &index, &params, &load));
                    head += consumed;
                    handled += 1;
                }
            }

            if handled >= keep_alive_requests {
                close_after_write = true;
                break;
            }
        }

        for response in responses.iter().copied() {
            stream.write_all(response).await?;
        }
        if close_after_write {
            return Ok(());
        }

        if head == tail {
            head = 0;
            tail = 0;
        } else if head > 0 && tail == rx.len() {
            rx.copy_within(head..tail, 0);
            tail -= head;
            head = 0;
        } else if tail == rx.len() {
            return Ok(());
        }

        let read = stream.read(&mut rx[tail..]).await?;
        if read == 0 {
            return Ok(());
        }
        tail += read;
    }
}

fn parse_request(buf: &[u8]) -> ParsedRequest {
    let header_end = match find_header_end(buf) {
        Some(pos) => pos,
        None => return ParsedRequest::Incomplete,
    };
    let line_end = match find_cr(buf, header_end) {
        Some(pos) => pos,
        None => return ParsedRequest::Bad,
    };
    let line = &buf[..line_end];

    if line.starts_with(b"POST ") {
        let path = &line[5..];
        if !path_eq(path, b"/fraud-score") {
            return ParsedRequest::NotFound {
                consumed: header_end + 4,
            };
        }

        let content_length = parse_content_length(&buf[line_end..header_end]).unwrap_or(0);
        let body_start = header_end + 4;
        let Some(body_end) = body_start.checked_add(content_length) else {
            return ParsedRequest::Bad;
        };
        if buf.len() < body_end {
            return ParsedRequest::Incomplete;
        }
        return ParsedRequest::Fraud {
            body_start,
            body_end,
            consumed: body_end,
        };
    }

    if line.starts_with(b"GET ") {
        let path = &line[4..];
        if path_eq(path, b"/ready") {
            return ParsedRequest::Ready {
                consumed: header_end + 4,
            };
        }
        return ParsedRequest::NotFound {
            consumed: header_end + 4,
        };
    }

    ParsedRequest::Bad
}

fn process_fraud(
    body: &[u8],
    index: &Index,
    params: &SearchParams,
    load: &AtomicUsize,
) -> &'static [u8] {
    if body.len() > MAX_REQUEST_BYTES {
        return RESP_APPROVED_0;
    }

    let Ok(body) = std::str::from_utf8(body) else {
        return RESP_APPROVED_0;
    };

    match parse_payload(body) {
        Ok(payload) => {
            let _guard = InFlightGuard::new(load);
            let query = vectorize(&payload);
            let classify_params = params.for_load(load.load(Ordering::Relaxed));
            let (approved, score) = index.classify(&query, &classify_params);
            fraud_response(approved, score)
        }
        Err(_) => RESP_APPROVED_0,
    }
}

fn fraud_response(approved: bool, score: f32) -> &'static [u8] {
    if approved {
        if score < 0.1 {
            RESP_APPROVED_0
        } else if score < 0.3 {
            RESP_APPROVED_02
        } else {
            RESP_APPROVED_04
        }
    } else if score < 0.7 {
        RESP_REJECTED_06
    } else if score < 0.9 {
        RESP_REJECTED_08
    } else {
        RESP_REJECTED_1
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    let mut pos = 0usize;
    while pos + 3 < buf.len() {
        if buf[pos] == b'\r'
            && buf[pos + 1] == b'\n'
            && buf[pos + 2] == b'\r'
            && buf[pos + 3] == b'\n'
        {
            return Some(pos);
        }
        pos += 1;
    }
    None
}

fn find_cr(buf: &[u8], limit: usize) -> Option<usize> {
    let mut pos = 0usize;
    while pos < limit {
        if buf[pos] == b'\r' {
            return Some(pos);
        }
        pos += 1;
    }
    None
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    const KEY: &[u8] = b"content-length:";
    if headers.len() < KEY.len() {
        return None;
    }

    let mut pos = 0usize;
    while pos + KEY.len() <= headers.len() {
        if eq_ascii_ci(&headers[pos..pos + KEY.len()], KEY) {
            let mut value_pos = pos + KEY.len();
            while value_pos < headers.len()
                && (headers[value_pos] == b' ' || headers[value_pos] == b'\t')
            {
                value_pos += 1;
            }

            let mut value = 0usize;
            while value_pos < headers.len() && headers[value_pos].is_ascii_digit() {
                value = value
                    .wrapping_mul(10)
                    .wrapping_add((headers[value_pos] - b'0') as usize);
                value_pos += 1;
            }
            return Some(value);
        }
        pos += 1;
    }

    None
}

fn eq_ascii_ci(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(&a, &b)| a.to_ascii_lowercase() == b)
}

fn path_eq(rest: &[u8], path: &[u8]) -> bool {
    if rest.len() < path.len() + 1 || &rest[..path.len()] != path {
        return false;
    }
    matches!(rest[path.len()], b' ' | b'?')
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
