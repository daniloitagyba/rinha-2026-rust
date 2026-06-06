#![cfg(unix)]

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;

pub struct ReceivedFd {
    pub fd: RawFd,
    pub initial: Vec<u8>,
}

pub fn receive_client_fd(
    control: &UnixStream,
    keep_initial: bool,
) -> io::Result<Option<ReceivedFd>> {
    receive_client_fd_raw(control.as_raw_fd(), keep_initial, 0)
}

pub fn receive_client_fd_raw(
    control_fd: RawFd,
    keep_initial: bool,
    flags: libc::c_int,
) -> io::Result<Option<ReceivedFd>> {
    let mut data = [0u8; 8192];
    let mut control_buf = [0u8; 64];

    loop {
        let mut iov = libc::iovec {
            iov_base: data.as_mut_ptr().cast(),
            iov_len: data.len(),
        };
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control_buf.as_mut_ptr().cast();
        msg.msg_controllen = control_buf.len();

        let received = unsafe { libc::recvmsg(control_fd, &mut msg, flags) };
        if received == 0 {
            return Ok(None);
        }
        if received < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if err.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(err);
        }

        let fd = unsafe { first_rights_fd(&msg) }
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing SCM_RIGHTS fd"))?;

        let initial = if keep_initial {
            data[..received as usize].to_vec()
        } else {
            Vec::new()
        };

        return Ok(Some(ReceivedFd { fd, initial }));
    }
}

unsafe fn first_rights_fd(msg: &libc::msghdr) -> Option<RawFd> {
    let mut cmsg = libc::CMSG_FIRSTHDR(msg);
    while !cmsg.is_null() {
        if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
            let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
            return Some(*data);
        }
        cmsg = libc::CMSG_NXTHDR(msg, cmsg);
    }
    None
}
