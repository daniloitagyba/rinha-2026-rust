#![cfg(unix)]

use crate::http::process_fraud_code;
use crate::index::{Index, SearchParams};
use std::fs;
use std::io;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

const MAX_RPC_BODY_BYTES: usize = 32 * 1024;
const RAW_RX_CAP: usize = 8192;
const RAW_TX_CAP: usize = 1024;
const MAX_EVENTS: usize = 1024;
const MAX_FD_SLOTS: usize = 65_536;

struct RawConn {
    rx: Vec<u8>,
    rx_head: usize,
    rx_tail: usize,
    tx: [u8; RAW_TX_CAP],
    tx_head: usize,
    tx_tail: usize,
    close_after_write: bool,
    registered_write: bool,
}

impl RawConn {
    fn new_box() -> Box<Self> {
        Box::new(Self {
            rx: vec![0u8; RAW_RX_CAP],
            rx_head: 0,
            rx_tail: 0,
            tx: [0; RAW_TX_CAP],
            tx_head: 0,
            tx_tail: 0,
            close_after_write: false,
            registered_write: false,
        })
    }

    fn reset(&mut self) {
        self.rx_head = 0;
        self.rx_tail = 0;
        self.tx_head = 0;
        self.tx_tail = 0;
        self.close_after_write = false;
        self.registered_write = false;
    }
}

struct RawConnTable {
    slots: Vec<Option<Box<RawConn>>>,
    pool: Vec<Box<RawConn>>,
}

impl RawConnTable {
    fn new() -> Self {
        let mut slots = Vec::with_capacity(MAX_FD_SLOTS);
        slots.resize_with(MAX_FD_SLOTS, || None);
        Self {
            slots,
            pool: Vec::with_capacity(512),
        }
    }

    fn insert(&mut self, fd: RawFd) -> bool {
        let idx = fd as usize;
        if idx >= self.slots.len() || self.slots[idx].is_some() {
            return false;
        }
        let mut conn = self.pool.pop().unwrap_or_else(RawConn::new_box);
        conn.reset();
        self.slots[idx] = Some(conn);
        true
    }

    fn get_mut(&mut self, fd: RawFd) -> Option<&mut RawConn> {
        self.slots.get_mut(fd as usize)?.as_deref_mut()
    }

    fn remove(&mut self, fd: RawFd) {
        let idx = fd as usize;
        if idx >= self.slots.len() {
            return;
        }
        if let Some(mut conn) = self.slots[idx].take() {
            conn.reset();
            if self.pool.len() < 512 {
                self.pool.push(conn);
            }
        }
    }
}

pub fn serve_rpc_raw(
    socket_path: &str,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
) -> Result<(), String> {
    if let Some(parent) = Path::new(socket_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create socket dir {}: {e}", parent.display()))?;
    }
    let _ = fs::remove_file(socket_path);

    let listener_fd = create_listener(socket_path)?;
    set_socket_permissions(socket_path);

    let epfd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if epfd < 0 {
        return Err(format!(
            "failed to create rpc epoll: {}",
            io::Error::last_os_error()
        ));
    }

    epoll_add(epfd, listener_fd, interest(false))
        .map_err(|e| format!("failed to register rpc listener: {e}"))?;

    eprintln!("serving raw rpc classifier on {socket_path}");

    let mut events = vec![empty_event(); MAX_EVENTS];
    let mut conns = RawConnTable::new();

    loop {
        let ready =
            unsafe { libc::epoll_wait(epfd, events.as_mut_ptr(), events.len() as libc::c_int, 1) };
        if ready < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(format!("rpc epoll wait failed: {err}"));
        }

        for event in events.iter().take(ready as usize) {
            let fd = event.u64 as RawFd;
            let flags = event.events;

            if fd == listener_fd {
                accept_rpc(listener_fd, epfd, &mut conns)?;
                continue;
            }

            let mut should_close =
                flags & (libc::EPOLLERR | libc::EPOLLHUP | libc::EPOLLRDHUP) as u32 != 0;
            if let Some(conn) = conns.get_mut(fd) {
                if handle_raw_rpc(fd, flags, conn, &index, &params, &load).is_err() {
                    should_close = true;
                }
                should_close |= conn.close_after_write && conn.tx_head == conn.tx_tail;
            } else {
                should_close = true;
            }

            if should_close {
                conns.remove(fd);
                epoll_delete(epfd, fd);
                close_fd(fd);
            } else if let Some(conn) = conns.get_mut(fd) {
                let needs_write = conn.tx_head < conn.tx_tail;
                if conn.registered_write != needs_write {
                    let _ = epoll_mod(epfd, fd, interest(needs_write));
                    conn.registered_write = needs_write;
                }
            }
        }
    }
}

pub async fn serve_rpc(
    socket_path: &str,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
) -> Result<(), String> {
    if let Some(parent) = Path::new(socket_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create socket dir {}: {e}", parent.display()))?;
    }
    let _ = fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)
        .map_err(|e| format!("failed to bind rpc socket {socket_path}: {e}"))?;
    set_socket_permissions(socket_path);

    eprintln!("serving rpc classifier on {socket_path}");

    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| format!("rpc accept error: {e}"))?;
        let index = Arc::clone(&index);
        let params = Arc::clone(&params);
        let load = Arc::clone(&load);
        tokio::spawn(async move {
            if let Err(err) = serve_rpc_connection(stream, index, params, load).await {
                eprintln!("rpc connection error: {err}");
            }
        });
    }
}

async fn serve_rpc_connection(
    mut stream: UnixStream,
    index: Arc<Index>,
    params: Arc<SearchParams>,
    load: Arc<AtomicUsize>,
) -> io::Result<()> {
    let mut header = [0u8; 2];
    let mut body = vec![0u8; MAX_RPC_BODY_BYTES];

    loop {
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(err) => return Err(err),
        }

        let len = u16::from_le_bytes(header) as usize;
        if len > MAX_RPC_BODY_BYTES {
            return Ok(());
        }

        stream.read_exact(&mut body[..len]).await?;
        let code = process_fraud_code(&body[..len], &index, &params, &load);
        stream.write_all(&[code]).await?;
    }
}

fn set_socket_permissions(path: &str) {
    if let Ok(metadata) = fs::metadata(path) {
        let mut perms = metadata.permissions();
        perms.set_mode(0o777);
        let _ = fs::set_permissions(path, perms);
    }
}

fn create_listener(path: &str) -> Result<RawFd, String> {
    let fd = unsafe {
        libc::socket(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return Err(format!(
            "failed to create rpc socket: {}",
            io::Error::last_os_error()
        ));
    }

    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let bytes = Path::new(path).as_os_str().as_bytes();
    if bytes.len() + 1 > addr.sun_path.len() {
        close_fd(fd);
        return Err(format!("rpc socket path too long: {path}"));
    }
    for (dst, src) in addr.sun_path.iter_mut().zip(bytes.iter().copied()) {
        *dst = src as libc::c_char;
    }

    let len = (std::mem::size_of::<libc::sa_family_t>() + bytes.len() + 1) as libc::socklen_t;
    let bind_result = unsafe { libc::bind(fd, (&addr as *const libc::sockaddr_un).cast(), len) };
    if bind_result < 0 {
        let err = io::Error::last_os_error();
        close_fd(fd);
        return Err(format!("failed to bind rpc socket {path}: {err}"));
    }

    if unsafe { libc::listen(fd, 4096) } < 0 {
        let err = io::Error::last_os_error();
        close_fd(fd);
        return Err(format!("failed to listen on rpc socket {path}: {err}"));
    }

    Ok(fd)
}

fn accept_rpc(listener_fd: RawFd, epfd: RawFd, conns: &mut RawConnTable) -> Result<(), String> {
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
            if !conns.insert(fd) {
                close_fd(fd);
                continue;
            }
            if let Err(err) = epoll_add(epfd, fd, interest(false)) {
                conns.remove(fd);
                close_fd(fd);
                return Err(format!("failed to register rpc client: {err}"));
            }
            continue;
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        if matches!(err.kind(), io::ErrorKind::WouldBlock) {
            return Ok(());
        }
        return Err(format!("rpc accept error: {err}"));
    }
}

fn handle_raw_rpc(
    fd: RawFd,
    flags: u32,
    conn: &mut RawConn,
    index: &Index,
    params: &SearchParams,
    load: &AtomicUsize,
) -> io::Result<()> {
    if flags & libc::EPOLLOUT as u32 != 0 {
        flush_tx(fd, conn)?;
    }
    if flags & libc::EPOLLIN as u32 != 0 {
        read_rx(fd, conn)?;
        parse_frames(conn, index, params, load)?;
        flush_tx(fd, conn)?;
    }
    Ok(())
}

fn read_rx(fd: RawFd, conn: &mut RawConn) -> io::Result<()> {
    compact_rx(conn);
    while conn.rx_tail < conn.rx.len() {
        let spare = &mut conn.rx[conn.rx_tail..];
        let read = unsafe { libc::recv(fd, spare.as_mut_ptr().cast(), spare.len(), 0) };
        if read > 0 {
            conn.rx_tail += read as usize;
            continue;
        }
        if read == 0 {
            conn.close_after_write = true;
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        if matches!(err.kind(), io::ErrorKind::WouldBlock) {
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

fn parse_frames(
    conn: &mut RawConn,
    index: &Index,
    params: &SearchParams,
    load: &AtomicUsize,
) -> io::Result<()> {
    while conn.rx_tail.saturating_sub(conn.rx_head) >= 2 && conn.tx_tail < conn.tx.len() {
        let len = u16::from_le_bytes([conn.rx[conn.rx_head], conn.rx[conn.rx_head + 1]]) as usize;
        if len > MAX_RPC_BODY_BYTES {
            conn.close_after_write = true;
            return Ok(());
        }

        let frame_end = conn.rx_head + 2 + len;
        if frame_end > conn.rx_tail {
            break;
        }

        let body = &conn.rx[conn.rx_head + 2..frame_end];
        conn.tx[conn.tx_tail] = process_fraud_code(body, index, params, load);
        conn.tx_tail += 1;
        conn.rx_head = frame_end;
    }
    compact_rx(conn);
    Ok(())
}

fn compact_rx(conn: &mut RawConn) {
    if conn.rx_head == 0 {
        return;
    }
    if conn.rx_head == conn.rx_tail {
        conn.rx_head = 0;
        conn.rx_tail = 0;
        return;
    }
    conn.rx.copy_within(conn.rx_head..conn.rx_tail, 0);
    conn.rx_tail -= conn.rx_head;
    conn.rx_head = 0;
}

fn flush_tx(fd: RawFd, conn: &mut RawConn) -> io::Result<()> {
    while conn.tx_head < conn.tx_tail {
        let out = &conn.tx[conn.tx_head..conn.tx_tail];
        let sent = unsafe { libc::send(fd, out.as_ptr().cast(), out.len(), libc::MSG_NOSIGNAL) };
        if sent > 0 {
            conn.tx_head += sent as usize;
            continue;
        }
        if sent == 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "send returned zero",
            ));
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        if matches!(err.kind(), io::ErrorKind::WouldBlock) {
            return Ok(());
        }
        return Err(err);
    }

    conn.tx_head = 0;
    conn.tx_tail = 0;
    Ok(())
}

fn interest(write: bool) -> u32 {
    let mut events = libc::EPOLLIN as u32 | libc::EPOLLRDHUP as u32;
    if write {
        events |= libc::EPOLLOUT as u32;
    }
    events
}

fn epoll_add(epfd: RawFd, fd: RawFd, events: u32) -> io::Result<()> {
    let mut event = libc::epoll_event {
        events,
        u64: fd as u64,
    };
    if unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, &mut event) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn epoll_mod(epfd: RawFd, fd: RawFd, events: u32) -> io::Result<()> {
    let mut event = libc::epoll_event {
        events,
        u64: fd as u64,
    };
    if unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, &mut event) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn epoll_delete(epfd: RawFd, fd: RawFd) {
    let _ = unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
}

fn empty_event() -> libc::epoll_event {
    libc::epoll_event { events: 0, u64: 0 }
}

fn close_fd(fd: RawFd) {
    let _ = unsafe { libc::close(fd) };
}
