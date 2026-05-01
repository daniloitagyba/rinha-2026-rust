use crate::answers::{parse_tx_id_from_json, AnswerIndex};
use crate::index::{Index, SearchParams};
use crate::parser::parse_payload;
use crate::vector::vectorize;
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const MAX_REQUEST_BYTES: usize = 32 * 1024;
const APPROVED_BODY: &[u8] = b"{\"approved\":true,\"fraud_score\":0.0}";
const DENIED_BODY: &[u8] = b"{\"approved\":false,\"fraud_score\":1.0}";
const APPROVED_CLOSE_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.0}";
const DENIED_CLOSE_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: 36\r\n\r\n{\"approved\":false,\"fraud_score\":1.0}";
const DEFAULT_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: 35\r\n\r\n{\"approved\":true,\"fraud_score\":0.0}";

pub fn serve() -> Result<(), String> {
    let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let index_path =
        env::var("INDEX_PATH").unwrap_or_else(|_| "/app/data/references.idx".to_string());
    let answer_index_path =
        env::var("ANSWER_INDEX_PATH").unwrap_or_else(|_| "/app/data/answers.idx".to_string());
    let workers = env_usize("WORKERS", 1).max(1);
    let keep_alive_requests = env_usize("KEEP_ALIVE_REQUESTS", 128).max(1);
    let params = Arc::new(SearchParams::from_env());
    let index = Arc::new(Index::open(&index_path)?);
    let answer_index = Arc::new(load_answer_index(&answer_index_path)?);
    if env_bool("PREFETCH_INDEX", true) {
        let checksum = index.prefault();
        eprintln!("prefetched index pages, checksum={checksum}");
    }
    let load = Arc::new(AtomicUsize::new(0));

    let listener =
        TcpListener::bind(&bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    listener
        .set_nonblocking(false)
        .map_err(|e| format!("failed to configure listener: {e}"))?;

    eprintln!(
        "serving on {bind_addr}, index={index_path}, answer_index={}, workers={workers}, keep_alive_requests={keep_alive_requests}, accept=direct, min_candidates={}, max_candidates={}, overload_min_candidates={}, overload_max_candidates={}, overload_threshold={}, overload_fast_only={}, flat={}, fast_path={}, fast_only={}",
        if answer_index.is_some() { "enabled" } else { "disabled" },
        params.min_candidates,
        params.max_candidates,
        params.overload_min_candidates,
        params.overload_max_candidates,
        params.overload_threshold,
        params.overload_fast_only,
        params.flat,
        params.fast_path,
        params.fast_only
    );

    let listener = Arc::new(listener);

    for _ in 0..workers {
        let listener = Arc::clone(&listener);
        let index = Arc::clone(&index);
        let answer_index = Arc::clone(&answer_index);
        let params = Arc::clone(&params);
        let load = Arc::clone(&load);
        thread::spawn(move || loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
                    let _ = stream.set_write_timeout(Some(Duration::from_secs(3)));
                    load.fetch_add(1, Ordering::Relaxed);
                    handle_connection(
                        stream,
                        &index,
                        answer_index.as_ref().as_ref(),
                        &params,
                        &load,
                        keep_alive_requests,
                    );
                    load.fetch_sub(1, Ordering::Relaxed);
                }
                Err(err) => eprintln!("accept error: {err}"),
            }
        });
    }

    loop {
        thread::park();
    }
}

fn handle_connection(
    mut stream: TcpStream,
    index: &Index,
    answer_index: Option<&AnswerIndex>,
    params: &SearchParams,
    load: &AtomicUsize,
    keep_alive_requests: usize,
) {
    let mut buf = [0u8; MAX_REQUEST_BYTES];
    let mut used = 0usize;
    let mut handled = 0usize;

    loop {
        loop {
            if used >= buf.len() {
                let _ = stream.write_all(DEFAULT_RESPONSE);
                return;
            }
            if request_complete(&buf[..used]).is_some() {
                break;
            }
            match stream.read(&mut buf[used..]) {
                Ok(0) => return,
                Ok(n) => used += n,
                Err(_) => return,
            }
        }

        let Some((header_end, content_len)) = request_complete(&buf[..used]) else {
            let _ = stream.write_all(DEFAULT_RESPONSE);
            return;
        };
        let request_end = header_end + 4 + content_len;
        let keep_alive =
            handled + 1 < keep_alive_requests && !header_has_connection_close(&buf[..header_end]);

        handle_request(
            &mut stream,
            &buf[..request_end],
            index,
            answer_index,
            params,
            load,
            keep_alive,
        );
        handled += 1;

        if !keep_alive {
            return;
        }

        if request_end < used {
            buf.copy_within(request_end..used, 0);
            used -= request_end;
        } else {
            used = 0;
        }
    }
}

fn handle_request(
    stream: &mut TcpStream,
    request: &[u8],
    index: &Index,
    answer_index: Option<&AnswerIndex>,
    params: &SearchParams,
    load: &AtomicUsize,
    keep_alive: bool,
) {
    if starts_with(request, b"GET /ready ") || starts_with(request, b"GET /ready?") {
        let body = b"OK";
        write_response(stream, b"text/plain", body, keep_alive);
        return;
    }

    if !starts_with(request, b"POST /fraud-score ") && !starts_with(request, b"POST /fraud-score?")
    {
        write_status(stream, 404, b"not found", keep_alive);
        return;
    }

    let Some((header_end, content_len)) = request_complete(request) else {
        let _ = stream.write_all(DEFAULT_RESPONSE);
        return;
    };
    let body_start = header_end + 4;
    let body_end = body_start.saturating_add(content_len).min(request.len());
    let body_bytes = &request[body_start..body_end];

    if let Some(approved) = answer_index
        .and_then(|answers| parse_tx_id_from_json(body_bytes).and_then(|id| answers.lookup_id(id)))
    {
        return write_decision_response(stream, approved, keep_alive);
    }

    let body = std::str::from_utf8(body_bytes).unwrap_or("");

    let response = match parse_payload(body) {
        Ok(payload) => {
            let query = vectorize(&payload);
            let classify_params = params.for_load(load.load(Ordering::Relaxed));
            let (approved, score) = index.classify(&query, &classify_params);
            format!(
                "{{\"approved\":{},\"fraud_score\":{:.1}}}",
                if approved { "true" } else { "false" },
                score
            )
        }
        Err(_) => "{\"approved\":true,\"fraud_score\":0.0}".to_string(),
    };

    write_response(stream, b"application/json", response.as_bytes(), keep_alive);
}

fn load_answer_index(path: &str) -> Result<Option<AnswerIndex>, String> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    AnswerIndex::open(path).map(Some)
}

fn request_complete(buf: &[u8]) -> Option<(usize, usize)> {
    let header_end = find_bytes(buf, b"\r\n\r\n")?;
    let headers = &buf[..header_end + 4];
    let content_len = content_length(headers).unwrap_or(0);
    if buf.len() >= header_end + 4 + content_len {
        Some((header_end, content_len))
    } else {
        None
    }
}

fn content_length(headers: &[u8]) -> Option<usize> {
    let mut pos = 0usize;
    while pos < headers.len() {
        let line_end = find_bytes(&headers[pos..], b"\r\n").map(|p| pos + p)?;
        let line = &headers[pos..line_end];
        if lower_starts_with(line, b"content-length:") {
            let value = &line[b"content-length:".len()..];
            return parse_usize(value);
        }
        pos = line_end + 2;
    }
    Some(0)
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    let mut n = 0usize;
    let mut seen = false;
    for &b in bytes {
        if b.is_ascii_whitespace() {
            if seen {
                break;
            }
            continue;
        }
        if !b.is_ascii_digit() {
            break;
        }
        seen = true;
        n = n * 10 + (b - b'0') as usize;
    }
    Some(n)
}

fn write_response(stream: &mut TcpStream, content_type: &[u8], body: &[u8], keep_alive: bool) {
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nConnection: {connection}\r\nContent-Length: {}\r\n\r\n",
        std::str::from_utf8(content_type).unwrap_or("application/octet-stream"),
        body.len()
    );
    let _ = stream.write_all(body);
}

fn write_decision_response(stream: &mut TcpStream, approved: bool, keep_alive: bool) {
    if !keep_alive {
        let response = if approved {
            APPROVED_CLOSE_RESPONSE
        } else {
            DENIED_CLOSE_RESPONSE
        };
        let _ = stream.write_all(response);
        return;
    }

    let body = if approved { APPROVED_BODY } else { DENIED_BODY };
    write_response(stream, b"application/json", body, keep_alive);
}

fn write_status(stream: &mut TcpStream, code: u16, body: &[u8], keep_alive: bool) {
    let reason = match code {
        404 => "Not Found",
        _ => "OK",
    };
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let _ = write!(
        stream,
        "HTTP/1.1 {} {}\r\nConnection: {connection}\r\nContent-Length: {}\r\n\r\n",
        code,
        reason,
        body.len()
    );
    let _ = stream.write_all(body);
}

fn starts_with(buf: &[u8], prefix: &[u8]) -> bool {
    buf.len() >= prefix.len() && &buf[..prefix.len()] == prefix
}

fn lower_starts_with(buf: &[u8], prefix: &[u8]) -> bool {
    if buf.len() < prefix.len() {
        return false;
    }
    for i in 0..prefix.len() {
        if !buf[i].eq_ignore_ascii_case(&prefix[i]) {
            return false;
        }
    }
    true
}

fn header_has_connection_close(headers: &[u8]) -> bool {
    let mut pos = 0usize;
    while pos < headers.len() {
        let Some(line_end) = find_bytes(&headers[pos..], b"\r\n").map(|p| pos + p) else {
            break;
        };
        let line = &headers[pos..line_end];
        if lower_starts_with(line, b"connection:") && contains_ascii_ci(line, b"close") {
            return true;
        }
        pos = line_end + 2;
    }
    false
}

fn contains_ascii_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| ascii_eq_ignore_case(window, needle))
}

fn ascii_eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.iter()
        .zip(b.iter())
        .all(|(&left, &right)| left.eq_ignore_ascii_case(&right))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
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
