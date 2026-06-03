#![cfg(unix)]

use crate::fdpass;
use crate::http::{
    parse_request, process_fraud, ParsedRequest, RESP_BAD_REQUEST, RESP_NOT_FOUND, RESP_READY,
    RX_CAP,
};
use crate::index::{Index, SearchParams};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

const MAX_EVENTS: usize = 1024;

struct Conn {
    buf: Vec<u8>,
    head: usize,
    tail: usize,
    pending: Vec<u8>,
    pending_off: usize,
    handled: usize,
    close_after_write: bool,
}

impl Conn {
    fn new(initial: Vec<u8>) -> Self {
        let mut buf = vec![0u8; RX_CAP];
        let tail = initial.len().min(RX_CAP);
        if tail > 0 {
            buf[..tail].copy_from_slice(&initial[..tail]);
        }

        Self {
            buf,
            head: 0,
            tail,
            pending: Vec::with_capacity(8192),
            pending_off: 0,
            handled: 0,
            close_after_write: false,
        }
    }
}

pub fn serve_fd_epoll(
    control_path: &str,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
    keep_alive_requests: usize,
) -> Result<(), String> {
    if let Some(parent) = Path::new(control_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create socket dir {}: {e}", parent.display()))?;
    }
    let _ = fs::remove_file(control_path);

    let listener = UnixListener::bind(control_path)
        .map_err(|e| format!("failed to bind fd control socket {control_path}: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set fd control socket nonblocking: {e}"))?;
    set_unix_socket_permissions(control_path);

    let listener_fd = listener.as_raw_fd();
    let epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if epfd < 0 {
        return Err(format!(
            "failed to create epoll: {}",
            io::Error::last_os_error()
        ));
    }

    epoll_add(epfd, listener_fd, control_interest())
        .map_err(|e| format!("failed to register fd control listener: {e}"))?;

    let timeout_ms = env_i32("FD_EPOLL_TIMEOUT_MS", 1);
    let keep_initial = env_bool("FD_CONTROL_PREBUFFER", false);
    let mut events = vec![empty_event(); MAX_EVENTS];
    let mut controls = HashSet::<RawFd>::new();
    let mut clients = HashMap::<RawFd, Conn>::with_capacity(4096);

    eprintln!(
        "fd epoll raw enabled, control={control_path}, timeout_ms={timeout_ms}, keep_initial={keep_initial}"
    );

    loop {
        let ready =
            unsafe { libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as i32, timeout_ms) };
        if ready < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(format!("epoll wait failed: {err}"));
        }

        for event in events.iter().take(ready as usize) {
            let fd = event.u64 as RawFd;
            let flags = event.events;

            if fd == listener_fd {
                accept_controls(&listener, epfd, &mut controls)?;
                continue;
            }

            if controls.contains(&fd) {
                if is_closed_event(flags) {
                    controls.remove(&fd);
                    epoll_delete(epfd, fd);
                    close_fd(fd);
                    continue;
                }
                receive_passed_fds(
                    epfd,
                    fd,
                    keep_initial,
                    &mut clients,
                    &index,
                    &params,
                    &load,
                    keep_alive_requests,
                )?;
                continue;
            }

            let mut should_close = is_closed_event(flags);
            if let Some(conn) = clients.get_mut(&fd) {
                match handle_client(
                    fd,
                    flags,
                    conn,
                    &index,
                    &params,
                    &load,
                    keep_alive_requests,
                ) {
                    Ok(close) => should_close |= close,
                    Err(_) => should_close = true,
                }
            } else {
                should_close = true;
            }

            if should_close {
                clients.remove(&fd);
                epoll_delete(epfd, fd);
                close_fd(fd);
            } else if let Some(conn) = clients.get(&fd) {
                let _ = epoll_mod(epfd, fd, client_interest(!conn.pending.is_empty()));
            }
        }
    }
}

fn accept_controls(
    listener: &UnixListener,
    epfd: RawFd,
    controls: &mut HashSet<RawFd>,
) -> Result<(), String> {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_nonblocking(true)
                    .map_err(|e| format!("failed to set fd control stream nonblocking: {e}"))?;
                let fd = stream.into_raw_fd();
                if let Err(err) = epoll_add(epfd, fd, control_interest()) {
                    close_fd(fd);
                    return Err(format!("failed to register fd control stream: {err}"));
                }
                controls.insert(fd);
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("fd control accept error: {err}")),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn receive_passed_fds(
    epfd: RawFd,
    control_fd: RawFd,
    keep_initial: bool,
    clients: &mut HashMap<RawFd, Conn>,
    index: &Index,
    params: &SearchParams,
    load: &AtomicUsize,
    keep_alive_requests: usize,
) -> Result<(), String> {
    loop {
        let received =
            match fdpass::receive_client_fd_raw(control_fd, keep_initial, libc::MSG_DONTWAIT) {
                Ok(Some(received)) => received,
                Ok(None) => return Ok(()),
                Err(err) => return Err(format!("fd control receive error: {err}")),
            };

        let fd = received.fd;
        if let Err(err) = fdpass::set_nonblocking(fd) {
            close_fd(fd);
            return Err(format!("fd client nonblocking error: {err}"));
        }

        let mut conn = Conn::new(received.initial);
        if conn.tail > 0 {
            let _ = parse_available(&mut conn, index, params, load, keep_alive_requests);
            let _ = flush_pending(fd, &mut conn);
        }

        if conn.close_after_write && conn.pending.is_empty() {
            close_fd(fd);
            continue;
        }

        if let Err(err) = epoll_add(epfd, fd, client_interest(!conn.pending.is_empty())) {
            close_fd(fd);
            return Err(format!("failed to register fd client: {err}"));
        }
        clients.insert(fd, conn);
    }
}

fn handle_client(
    fd: RawFd,
    flags: u32,
    conn: &mut Conn,
    index: &Index,
    params: &SearchParams,
    load: &AtomicUsize,
    keep_alive_requests: usize,
) -> io::Result<bool> {
    if flags & libc::EPOLLOUT as u32 != 0 {
        flush_pending(fd, conn)?;
    }
    if conn.close_after_write && conn.pending.is_empty() {
        return Ok(true);
    }

    if flags & libc::EPOLLIN as u32 != 0 {
        read_available(fd, conn)?;
        parse_available(conn, index, params, load, keep_alive_requests);
        flush_pending(fd, conn)?;
    }

    Ok(conn.close_after_write && conn.pending.is_empty())
}

fn read_available(fd: RawFd, conn: &mut Conn) -> io::Result<()> {
    loop {
        if conn.tail == conn.buf.len() {
            if conn.head > 0 {
                conn.buf.copy_within(conn.head..conn.tail, 0);
                conn.tail -= conn.head;
                conn.head = 0;
            } else {
                conn.close_after_write = true;
                return Ok(());
            }
        }

        let spare = &mut conn.buf[conn.tail..];
        let read = unsafe { libc::recv(fd, spare.as_mut_ptr().cast(), spare.len(), 0) };
        if read == 0 {
            conn.close_after_write = true;
            return Ok(());
        }
        if read < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if matches!(err.kind(), io::ErrorKind::WouldBlock) {
                return Ok(());
            }
            return Err(err);
        }
        conn.tail += read as usize;
    }
}

fn parse_available(
    conn: &mut Conn,
    index: &Index,
    params: &SearchParams,
    load: &AtomicUsize,
    keep_alive_requests: usize,
) {
    while conn.head < conn.tail {
        match parse_request(&conn.buf[conn.head..conn.tail]) {
            ParsedRequest::Incomplete => break,
            ParsedRequest::Bad => {
                conn.pending.extend_from_slice(RESP_BAD_REQUEST);
                conn.close_after_write = true;
                conn.head = conn.tail;
                break;
            }
            ParsedRequest::Ready { consumed } => {
                conn.pending.extend_from_slice(RESP_READY);
                conn.head += consumed;
                conn.handled += 1;
            }
            ParsedRequest::NotFound { consumed } => {
                conn.pending.extend_from_slice(RESP_NOT_FOUND);
                conn.head += consumed;
                conn.handled += 1;
            }
            ParsedRequest::Fraud {
                body_start,
                body_end,
                consumed,
            } => {
                let response = {
                    let body = &conn.buf[conn.head + body_start..conn.head + body_end];
                    process_fraud(body, index, params, load)
                };
                conn.pending.extend_from_slice(response);
                conn.head += consumed;
                conn.handled += 1;
            }
        }

        if conn.handled >= keep_alive_requests {
            conn.close_after_write = true;
            break;
        }
    }

    if conn.head == conn.tail {
        conn.head = 0;
        conn.tail = 0;
    } else if conn.head > 0 && conn.tail == conn.buf.len() {
        conn.buf.copy_within(conn.head..conn.tail, 0);
        conn.tail -= conn.head;
        conn.head = 0;
    }
}

fn flush_pending(fd: RawFd, conn: &mut Conn) -> io::Result<()> {
    while conn.pending_off < conn.pending.len() {
        let out = &conn.pending[conn.pending_off..];
        let sent = unsafe { libc::send(fd, out.as_ptr().cast(), out.len(), send_flags()) };
        if sent == 0 {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "send returned zero"));
        }
        if sent < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if matches!(err.kind(), io::ErrorKind::WouldBlock) {
                return Ok(());
            }
            return Err(err);
        }
        conn.pending_off += sent as usize;
    }

    if conn.pending_off > 0 {
        conn.pending.clear();
        conn.pending_off = 0;
    }
    Ok(())
}

fn epoll_add(epfd: RawFd, fd: RawFd, events: u32) -> io::Result<()> {
    epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, events)
}

fn epoll_mod(epfd: RawFd, fd: RawFd, events: u32) -> io::Result<()> {
    epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, events)
}

fn epoll_delete(epfd: RawFd, fd: RawFd) {
    let _ = epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, 0);
}

fn epoll_ctl(epfd: RawFd, op: libc::c_int, fd: RawFd, events: u32) -> io::Result<()> {
    let mut event = libc::epoll_event {
        events,
        u64: fd as u64,
    };
    if unsafe { libc::epoll_ctl(epfd, op, fd, &mut event) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn control_interest() -> u32 {
    (libc::EPOLLIN | libc::EPOLLERR | libc::EPOLLHUP) as u32
}

fn client_interest(has_pending: bool) -> u32 {
    let mut events = (libc::EPOLLIN | libc::EPOLLERR | libc::EPOLLHUP) as u32;
    if has_pending {
        events |= libc::EPOLLOUT as u32;
    }
    #[cfg(target_os = "linux")]
    {
        events |= libc::EPOLLRDHUP as u32;
    }
    events
}

fn is_closed_event(flags: u32) -> bool {
    let mut closed = flags & (libc::EPOLLERR as u32 | libc::EPOLLHUP as u32) != 0;
    #[cfg(target_os = "linux")]
    {
        closed |= flags & libc::EPOLLRDHUP as u32 != 0;
    }
    closed
}

fn close_fd(fd: RawFd) {
    unsafe {
        let _ = libc::close(fd);
    }
}

fn empty_event() -> libc::epoll_event {
    libc::epoll_event { events: 0, u64: 0 }
}

#[cfg(target_os = "linux")]
fn send_flags() -> libc::c_int {
    libc::MSG_NOSIGNAL
}

#[cfg(not(target_os = "linux"))]
fn send_flags() -> libc::c_int {
    0
}

fn env_i32(name: &str, default: i32) -> i32 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn set_unix_socket_permissions(path: &str) {
    let permissions = fs::Permissions::from_mode(0o666);
    let _ = fs::set_permissions(path, permissions);
}
