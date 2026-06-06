#![cfg(unix)]

use crate::fdpass;
use crate::http::{
    parse_request, process_fraud, ParsedRequest, RESP_BAD_REQUEST, RESP_NOT_FOUND, RESP_READY,
    RX_CAP,
};
use crate::index::{Index, SearchParams};
use std::env;
use std::fs;
use std::io;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Instant;

const MAX_EVENTS: usize = 1024;
const MAX_FD_SLOTS: usize = 65_536;

#[derive(Clone, Copy)]
struct WaitTuning {
    timeout_ms: i32,
    spin_us: usize,
    idle_us: usize,
}

#[derive(Clone, Copy)]
struct BusyPollTuning {
    usecs: u32,
    budget: u16,
    prefer: u8,
}

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
    fn new_box() -> Box<Self> {
        Box::new(Self {
            buf: vec![0u8; RX_CAP],
            head: 0,
            tail: 0,
            pending: Vec::with_capacity(8192),
            pending_off: 0,
            handled: 0,
            close_after_write: false,
        })
    }

    fn reset(&mut self, initial: Vec<u8>) {
        let tail = initial.len().min(RX_CAP);
        if tail > 0 {
            self.buf[..tail].copy_from_slice(&initial[..tail]);
        }

        self.head = 0;
        self.tail = tail;
        self.pending.clear();
        self.pending_off = 0;
        self.handled = 0;
        self.close_after_write = false;
    }
}

struct ConnTable {
    slots: Vec<Option<Box<Conn>>>,
    pool: Vec<Box<Conn>>,
    pool_cap: usize,
}

impl ConnTable {
    fn new(pool_cap: usize) -> Self {
        let mut pool = Vec::with_capacity(pool_cap);
        for _ in 0..pool_cap.min(128) {
            pool.push(Conn::new_box());
        }

        let mut slots = Vec::with_capacity(MAX_FD_SLOTS);
        slots.resize_with(MAX_FD_SLOTS, || None);

        Self {
            slots,
            pool,
            pool_cap,
        }
    }

    fn insert(&mut self, fd: RawFd, initial: Vec<u8>) -> bool {
        let idx = fd as usize;
        if idx >= self.slots.len() || self.slots[idx].is_some() {
            return false;
        }

        let mut conn = self.pool.pop().unwrap_or_else(Conn::new_box);
        conn.reset(initial);
        self.slots[idx] = Some(conn);
        true
    }

    fn get(&self, fd: RawFd) -> Option<&Conn> {
        self.slots.get(fd as usize)?.as_deref()
    }

    fn get_mut(&mut self, fd: RawFd) -> Option<&mut Conn> {
        self.slots.get_mut(fd as usize)?.as_deref_mut()
    }

    fn remove(&mut self, fd: RawFd) {
        let idx = fd as usize;
        if idx >= self.slots.len() {
            return;
        }

        if let Some(mut conn) = self.slots[idx].take() {
            conn.reset(Vec::new());
            if self.pool.len() < self.pool_cap {
                self.pool.push(conn);
            }
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

    let listener_fd = create_control_listener(control_path)?;
    set_unix_socket_permissions(control_path);

    let epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if epfd < 0 {
        return Err(format!(
            "failed to create epoll: {}",
            io::Error::last_os_error()
        ));
    }
    let busy_poll = configure_busy_poll(epfd);

    epoll_add(epfd, listener_fd, control_interest())
        .map_err(|e| format!("failed to register fd control listener: {e}"))?;

    let wait = WaitTuning {
        timeout_ms: env_i32("FD_EPOLL_TIMEOUT_MS", 1),
        spin_us: env_usize("FD_EPOLL_SPIN_US", 0),
        idle_us: env_usize("FD_EPOLL_IDLE_US", 0),
    };
    let keep_initial = env_bool("FD_CONTROL_PREBUFFER", false);
    let conn_pool_cap = env_usize("FD_CONN_POOL_CAP", 512);
    let mut events = vec![empty_event(); MAX_EVENTS];
    let mut controls = Vec::<RawFd>::with_capacity(4);
    let mut clients = ConnTable::new(conn_pool_cap);

    eprintln!(
        "fd epoll raw enabled, control={control_path}, timeout_ms={}, spin_us={}, idle_us={}, busy_poll_us={}, busy_poll_budget={}, prefer_busy_poll={}, keep_initial={keep_initial}, conn_pool_cap={conn_pool_cap}",
        wait.timeout_ms,
        wait.spin_us,
        wait.idle_us,
        busy_poll.usecs,
        busy_poll.budget,
        busy_poll.prefer
    );

    loop {
        let ready = wait_events(epfd, &mut events, wait)
            .map_err(|err| format!("epoll wait failed: {err}"))?;

        for event in events.iter().take(ready as usize) {
            let fd = event.u64 as RawFd;
            let flags = event.events;

            if fd == listener_fd {
                accept_controls(listener_fd, epfd, &mut controls)?;
                continue;
            }

            if let Some(control_idx) = controls.iter().position(|&control| control == fd) {
                if is_closed_event(flags) {
                    controls.swap_remove(control_idx);
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
            if let Some(conn) = clients.get_mut(fd) {
                match handle_client(fd, flags, conn, &index, &params, &load, keep_alive_requests) {
                    Ok(close) => should_close |= close,
                    Err(_) => should_close = true,
                }
            } else {
                should_close = true;
            }

            if should_close {
                clients.remove(fd);
                epoll_delete(epfd, fd);
                close_fd(fd);
            } else if let Some(conn) = clients.get(fd) {
                let _ = epoll_mod(epfd, fd, client_interest(!conn.pending.is_empty()));
            }
        }
    }
}

fn accept_controls(
    listener_fd: RawFd,
    epfd: RawFd,
    controls: &mut Vec<RawFd>,
) -> Result<(), String> {
    loop {
        let fd = unsafe {
            libc::accept4(
                listener_fd,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            )
        };
        if fd >= 0 {
            if let Err(err) = epoll_add(epfd, fd, control_interest()) {
                close_fd(fd);
                return Err(format!("failed to register fd control stream: {err}"));
            }
            controls.push(fd);
            continue;
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        if matches!(err.kind(), io::ErrorKind::WouldBlock) {
            return Ok(());
        }
        return Err(format!("fd control accept error: {err}"));
    }
}

#[allow(clippy::too_many_arguments)]
fn receive_passed_fds(
    epfd: RawFd,
    control_fd: RawFd,
    keep_initial: bool,
    clients: &mut ConnTable,
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
        if !clients.insert(fd, received.initial) {
            close_fd(fd);
            continue;
        }

        let mut close_now = false;
        let mut has_pending = false;
        let mut client_error = false;
        if let Some(conn) = clients.get_mut(fd) {
            if conn.tail > 0 {
                if parse_available(fd, conn, index, params, load, keep_alive_requests).is_err()
                    || flush_pending(fd, conn).is_err()
                {
                    client_error = true;
                }
            }
            close_now = conn.close_after_write && conn.pending.is_empty();
            has_pending = !conn.pending.is_empty();
        }

        if client_error {
            clients.remove(fd);
            close_fd(fd);
            continue;
        }

        if close_now {
            clients.remove(fd);
            close_fd(fd);
            continue;
        }

        if let Err(err) = epoll_add(epfd, fd, client_interest(has_pending)) {
            clients.remove(fd);
            close_fd(fd);
            return Err(format!("failed to register fd client: {err}"));
        }
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
        parse_available(fd, conn, index, params, load, keep_alive_requests)?;
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
    fd: RawFd,
    conn: &mut Conn,
    index: &Index,
    params: &SearchParams,
    load: &AtomicUsize,
    keep_alive_requests: usize,
) -> io::Result<()> {
    while conn.head < conn.tail {
        match parse_request(&conn.buf[conn.head..conn.tail]) {
            ParsedRequest::Incomplete => break,
            ParsedRequest::Bad => {
                write_or_buffer(fd, conn, RESP_BAD_REQUEST)?;
                conn.close_after_write = true;
                conn.head = conn.tail;
                break;
            }
            ParsedRequest::Ready { consumed } => {
                write_or_buffer(fd, conn, RESP_READY)?;
                conn.head += consumed;
                conn.handled += 1;
            }
            ParsedRequest::NotFound { consumed } => {
                write_or_buffer(fd, conn, RESP_NOT_FOUND)?;
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
                write_or_buffer(fd, conn, response)?;
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
    Ok(())
}

fn write_or_buffer(fd: RawFd, conn: &mut Conn, response: &'static [u8]) -> io::Result<()> {
    if !conn.pending.is_empty() {
        conn.pending.extend_from_slice(response);
        return Ok(());
    }

    let mut off = 0usize;
    while off < response.len() {
        let sent = unsafe {
            libc::send(
                fd,
                response[off..].as_ptr().cast(),
                response.len() - off,
                send_flags(),
            )
        };
        if sent == 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "send returned zero",
            ));
        }
        if sent < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if matches!(err.kind(), io::ErrorKind::WouldBlock) {
                conn.pending.extend_from_slice(&response[off..]);
                return Ok(());
            }
            return Err(err);
        }
        off += sent as usize;
    }
    Ok(())
}

fn flush_pending(fd: RawFd, conn: &mut Conn) -> io::Result<()> {
    while conn.pending_off < conn.pending.len() {
        let out = &conn.pending[conn.pending_off..];
        let sent = unsafe { libc::send(fd, out.as_ptr().cast(), out.len(), send_flags()) };
        if sent == 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "send returned zero",
            ));
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

fn create_control_listener(control_path: &str) -> Result<RawFd, String> {
    let socket_type = if env_bool("FD_CONTROL_SEQPACKET", false) {
        libc::SOCK_SEQPACKET
    } else {
        libc::SOCK_STREAM
    };
    let fd = unsafe {
        libc::socket(
            libc::AF_UNIX,
            socket_type | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return Err(format!(
            "failed to create fd control socket {control_path}: {}",
            io::Error::last_os_error()
        ));
    }

    let path_bytes = Path::new(control_path).as_os_str().as_bytes();
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    if path_bytes.len() >= addr.sun_path.len() {
        close_fd(fd);
        return Err(format!("fd control socket path too long: {control_path}"));
    }
    for (slot, byte) in addr.sun_path.iter_mut().zip(path_bytes.iter()) {
        *slot = *byte as libc::c_char;
    }
    let len = (std::mem::size_of::<libc::sa_family_t>() + path_bytes.len() + 1) as libc::socklen_t;

    if unsafe { libc::bind(fd, (&addr as *const libc::sockaddr_un).cast(), len) } < 0 {
        let err = io::Error::last_os_error();
        close_fd(fd);
        return Err(format!(
            "failed to bind fd control socket {control_path}: {err}"
        ));
    }
    if unsafe { libc::listen(fd, env_i32("FD_CONTROL_BACKLOG", 4096)) } < 0 {
        let err = io::Error::last_os_error();
        close_fd(fd);
        return Err(format!(
            "failed to listen on fd control socket {control_path}: {err}"
        ));
    }

    Ok(fd)
}

fn configure_busy_poll(epfd: RawFd) -> BusyPollTuning {
    let tuning = BusyPollTuning {
        usecs: env_u32_any("FD_EPOLL_BUSY_POLL_US", "EPOLL_BUSY_POLL_US", 0),
        budget: env_u32_any("FD_EPOLL_BUSY_POLL_BUDGET", "EPOLL_BUSY_POLL_BUDGET", 8) as u16,
        prefer: env_u32_any("FD_EPOLL_PREFER_BUSY_POLL", "EPOLL_PREFER_BUSY_POLL", 1) as u8,
    };

    #[cfg(target_os = "linux")]
    {
        if tuning.usecs == 0 && tuning.prefer == 0 {
            return tuning;
        }

        #[repr(C)]
        struct EpollParams {
            busy_poll_usecs: u32,
            busy_poll_budget: u16,
            prefer_busy_poll: u8,
            _pad: u8,
        }

        const fn iow(ty: u32, nr: u32, size: u32) -> libc::c_ulong {
            ((1u32 << 30) | (size << 16) | (ty << 8) | nr) as libc::c_ulong
        }

        const EPIOCSPARAMS: libc::c_ulong =
            iow(0x8A, 0x01, std::mem::size_of::<EpollParams>() as u32);

        let params = EpollParams {
            busy_poll_usecs: tuning.usecs,
            busy_poll_budget: tuning.budget,
            prefer_busy_poll: tuning.prefer,
            _pad: 0,
        };

        unsafe {
            libc::ioctl(epfd, EPIOCSPARAMS, &params as *const EpollParams);
        }
    }

    tuning
}

fn wait_events(epfd: RawFd, events: &mut [libc::epoll_event], wait: WaitTuning) -> io::Result<i32> {
    if wait.spin_us == 0 && wait.idle_us == 0 {
        return epoll_wait_ms(epfd, events, wait.timeout_ms);
    }

    let mut ready = epoll_wait_ms(epfd, events, 0)?;
    if ready != 0 {
        return Ok(ready);
    }

    if wait.spin_us > 0 {
        let start = Instant::now();
        while start.elapsed().as_micros() < wait.spin_us as u128 {
            ready = epoll_wait_ms(epfd, events, 0)?;
            if ready != 0 {
                return Ok(ready);
            }
            std::hint::spin_loop();
        }
    }

    if wait.idle_us == 0 {
        epoll_wait_ms(epfd, events, -1)
    } else {
        epoll_wait_us(epfd, events, wait.idle_us)
    }
}

fn epoll_wait_ms(
    epfd: RawFd,
    events: &mut [libc::epoll_event],
    timeout_ms: i32,
) -> io::Result<i32> {
    loop {
        let ready =
            unsafe { libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as i32, timeout_ms) };
        if ready >= 0 {
            return Ok(ready);
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(err);
    }
}

#[cfg(target_os = "linux")]
fn epoll_wait_us(
    epfd: RawFd,
    events: &mut [libc::epoll_event],
    timeout_us: usize,
) -> io::Result<i32> {
    let ts = libc::timespec {
        tv_sec: (timeout_us / 1_000_000) as libc::time_t,
        tv_nsec: ((timeout_us % 1_000_000) * 1000) as libc::c_long,
    };
    loop {
        let ready = unsafe {
            libc::epoll_pwait2(
                epfd,
                events.as_mut_ptr(),
                events.len() as i32,
                &ts,
                std::ptr::null(),
            )
        };
        if ready >= 0 {
            return Ok(ready);
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        if err.raw_os_error() == Some(libc::ENOSYS) {
            let timeout_ms = timeout_us.div_ceil(1000).min(i32::MAX as usize) as i32;
            return epoll_wait_ms(epfd, events, timeout_ms);
        }
        return Err(err);
    }
}

#[cfg(not(target_os = "linux"))]
fn epoll_wait_us(
    epfd: RawFd,
    events: &mut [libc::epoll_event],
    timeout_us: usize,
) -> io::Result<i32> {
    let timeout_ms = timeout_us.div_ceil(1000).min(i32::MAX as usize) as i32;
    epoll_wait_ms(epfd, events, timeout_ms)
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

fn env_u32_any(primary: &str, fallback_name: &str, default: u32) -> u32 {
    env::var(primary)
        .ok()
        .or_else(|| env::var(fallback_name).ok())
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
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

fn set_unix_socket_permissions(path: &str) {
    let permissions = fs::Permissions::from_mode(0o666);
    let _ = fs::set_permissions(path, permissions);
}
