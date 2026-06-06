#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define MAX_BACKENDS 8
#define MAX_EVENTS 64
#define MAX_FD_SLOTS 65536
#define PROXY_BUF_SIZE 8192

static const char *control_paths[MAX_BACKENDS] = {
    "/sockets/api1.sock.ctrl",
    "/sockets/api2.sock.ctrl",
};

static char upstreams_storage[1024];
static int control_fds[MAX_BACKENDS];
static unsigned int backend_count = 2;
static unsigned int next_backend = 0;
static int fd_control_seqpacket = 0;
static int lb_preconnect_control = 1;
static int lb_tcp_nodelay = 1;
static int lb_socket_buffers = 1;
static int lb_sendmsg_dontwait = 0;
static int lb_prebuffer_initial = 0;
static int socket_buffer_size = 16384;

struct fd_backend {
    int fd;
    char dummy;
    struct iovec iov;
    char control[CMSG_SPACE(sizeof(int))];
    struct msghdr msg;
    struct cmsghdr *cmsg;
};

static struct fd_backend fd_backends[MAX_BACKENDS];

struct proxy_conn {
    int client_fd;
    int backend_fd;
    int connecting;
    int client_eof;
    int backend_eof;
    size_t c2b_head;
    size_t c2b_tail;
    size_t b2c_head;
    size_t b2c_tail;
    char c2b[PROXY_BUF_SIZE];
    char b2c[PROXY_BUF_SIZE];
};

static struct proxy_conn *proxy_by_fd[MAX_FD_SLOTS];

struct rpc_conn {
    int client_fd;
    int backend_fd;
    int connecting;
    int client_eof;
    int backend_eof;
    int awaiting_response;
    size_t in_head;
    size_t in_tail;
    size_t out_head;
    size_t out_tail;
    size_t rpc_head;
    size_t rpc_tail;
    char in[PROXY_BUF_SIZE];
    char out[PROXY_BUF_SIZE];
    char rpc[PROXY_BUF_SIZE];
};

static struct rpc_conn *rpc_by_fd[MAX_FD_SLOTS];

static const char resp_ready[] = "HTTP/1.1 200 OK\r\nContent-Length:0\r\n\r\n";
static const char resp_not_found[] = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
static const char resp_bad_request[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
static const char resp_approved_0[] = "HTTP/1.1 200 OK\r\nContent-Length:17\r\n\r\n{\"approved\":true}";
static const char resp_approved_02[] = "HTTP/1.1 200 OK\r\nContent-Length:17\r\n\r\n{\"approved\":true}";
static const char resp_approved_04[] = "HTTP/1.1 200 OK\r\nContent-Length:17\r\n\r\n{\"approved\":true}";
static const char resp_rejected_06[] = "HTTP/1.1 200 OK\r\nContent-Length:18\r\n\r\n{\"approved\":false}";
static const char resp_rejected_08[] = "HTTP/1.1 200 OK\r\nContent-Length:18\r\n\r\n{\"approved\":false}";
static const char resp_rejected_1[] = "HTTP/1.1 200 OK\r\nContent-Length:18\r\n\r\n{\"approved\":false}";

static int env_enabled(const char *name, int fallback) {
    const char *value = getenv(name);
    if (value == NULL || *value == '\0') {
        return fallback;
    }

    return strcmp(value, "0") != 0 &&
           strcmp(value, "false") != 0 &&
           strcmp(value, "FALSE") != 0 &&
           strcmp(value, "no") != 0 &&
           strcmp(value, "NO") != 0;
}

static int env_int(const char *name, int fallback) {
    const char *value = getenv(name);
    if (value == NULL || *value == '\0') {
        return fallback;
    }

    int parsed = atoi(value);
    return parsed > 0 ? parsed : fallback;
}

static void set_small_socket_buffers(int fd) {
    int value = socket_buffer_size;
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &value, sizeof(value));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &value, sizeof(value));
}

static void configure_client(int fd) {
    int one = 1;
    if (lb_tcp_nodelay) {
        (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        (void)setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &one, sizeof(one));
    }
    if (lb_socket_buffers) {
        set_small_socket_buffers(fd);
    }
}

static void init_control_fds(void) {
    for (int i = 0; i < MAX_BACKENDS; i++) {
        control_fds[i] = -1;
        fd_backends[i].fd = -1;
    }
}

static void init_fd_backend(unsigned int index, int fd) {
    struct fd_backend *backend = &fd_backends[index];
    memset(backend, 0, sizeof(*backend));
    backend->fd = fd;
    backend->dummy = 1;
    backend->iov.iov_base = &backend->dummy;
    backend->iov.iov_len = 1;
    backend->msg.msg_iov = &backend->iov;
    backend->msg.msg_iovlen = 1;
    backend->msg.msg_control = backend->control;
    backend->msg.msg_controllen = sizeof(backend->control);
    backend->cmsg = CMSG_FIRSTHDR(&backend->msg);
    backend->cmsg->cmsg_level = SOL_SOCKET;
    backend->cmsg->cmsg_type = SCM_RIGHTS;
    backend->cmsg->cmsg_len = CMSG_LEN(sizeof(int));
}

static void init_backends(void) {
    const char *upstreams = getenv("UPSTREAMS");
    if (upstreams == NULL || *upstreams == '\0') {
        return;
    }

    strncpy(upstreams_storage, upstreams, sizeof(upstreams_storage) - 1);
    upstreams_storage[sizeof(upstreams_storage) - 1] = '\0';

    unsigned int count = 0;
    char *save = NULL;
    char *item = strtok_r(upstreams_storage, ",", &save);
    while (item != NULL && count < MAX_BACKENDS) {
        while (*item == ' ' || *item == '\t') {
            item++;
        }

        char *end = item + strlen(item);
        while (end > item && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' || end[-1] == '\r')) {
            *--end = '\0';
        }

        if (*item != '\0') {
            control_paths[count++] = item;
        }

        item = strtok_r(NULL, ",", &save);
    }

    if (count > 0) {
        backend_count = count;
    }
}

static int connect_control(unsigned int index) {
    int socket_type = fd_control_seqpacket ? SOCK_SEQPACKET : SOCK_STREAM;
    int fd = socket(AF_UNIX, socket_type | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }

    int sndbuf = 256 * 1024;
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, control_paths[index], sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
        return fd;
    }

    close(fd);
    return -1;
}

static int connect_backend_stream(unsigned int index, int *connecting) {
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, control_paths[index], sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
        *connecting = 0;
        return fd;
    }

    if (errno == EINPROGRESS || errno == EAGAIN) {
        *connecting = 1;
        return fd;
    }

    close(fd);
    return -1;
}

static int ensure_control(unsigned int index) {
    if (control_fds[index] >= 0) {
        return control_fds[index];
    }

    int fd = connect_control(index);
    control_fds[index] = fd;
    if (fd >= 0) {
        init_fd_backend(index, fd);
    }
    return fd;
}

static void preconnect_controls(void) {
    if (!lb_preconnect_control) {
        return;
    }

    for (unsigned int i = 0; i < backend_count; i++) {
        (void)ensure_control(i);
    }
}

static unsigned int choose_backend(void) {
    unsigned int backend = next_backend;
    next_backend = (next_backend + 1) % backend_count;
    return backend;
}

static int send_fd_with_flags(int control_fd, int fd_to_send, const char *initial, size_t initial_len, int flags) {
    char small_data = 0;
    char prebuffered_data[8193];
    void *iov_base = &small_data;
    size_t iov_len = 1;
    if (initial_len > 0) {
        prebuffered_data[0] = 0;
        if (initial_len > sizeof(prebuffered_data) - 1) {
            initial_len = sizeof(prebuffered_data) - 1;
        }
        memcpy(prebuffered_data + 1, initial, initial_len);
        iov_base = prebuffered_data;
        iov_len = initial_len + 1;
    }

    struct iovec io;
    io.iov_base = iov_base;
    io.iov_len = iov_len;

    char cmsgbuf[CMSG_SPACE(sizeof(int))];
    memset(cmsgbuf, 0, sizeof(cmsgbuf));

    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsgbuf;
    msg.msg_controllen = sizeof(cmsgbuf);

    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &fd_to_send, sizeof(int));
    msg.msg_controllen = cmsg->cmsg_len;

    for (;;) {
        ssize_t sent = sendmsg(control_fd, &msg, flags);
        if (sent == (ssize_t)io.iov_len) {
            return 0;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 1;
        }
        return -1;
    }
}

static int send_fd_prebuilt_with_flags(struct fd_backend *backend, int fd_to_send, int flags) {
    backend->msg.msg_controllen = sizeof(backend->control);
    memcpy(CMSG_DATA(backend->cmsg), &fd_to_send, sizeof(int));

    for (;;) {
        ssize_t sent = sendmsg(backend->fd, &backend->msg, flags);
        if (sent == (ssize_t)backend->iov.iov_len) {
            return 0;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 1;
        }
        return -1;
    }
}

static int send_fd(int control_fd, int fd_to_send, const char *initial, size_t initial_len) {
    int flags = MSG_NOSIGNAL;
    if (lb_sendmsg_dontwait) {
        flags |= MSG_DONTWAIT;
    }
    return send_fd_with_flags(control_fd, fd_to_send, initial, initial_len, flags);
}

static int deliver_fd(int client_fd, const char *initial, size_t initial_len) {
    unsigned int start = choose_backend();
    for (unsigned int attempt = 0; attempt < backend_count; attempt++) {
        unsigned int index = (start + attempt) % backend_count;
        int control_fd = ensure_control(index);
        if (control_fd >= 0) {
            int result = (initial_len == 0 && fd_backends[index].fd >= 0)
                ? send_fd_prebuilt_with_flags(&fd_backends[index], client_fd, MSG_NOSIGNAL | (lb_sendmsg_dontwait ? MSG_DONTWAIT : 0))
                : send_fd(control_fd, client_fd, initial, initial_len);
            if (result == 0) {
                return 0;
            }
            if (result > 0) {
                continue;
            }
        }

        if (control_fds[index] >= 0) {
            close(control_fds[index]);
            control_fds[index] = -1;
            fd_backends[index].fd = -1;
        }
    }

    if (lb_sendmsg_dontwait) {
        int control_fd = ensure_control(start);
        if (control_fd >= 0 && (
            (initial_len == 0 && fd_backends[start].fd >= 0
                && send_fd_prebuilt_with_flags(&fd_backends[start], client_fd, MSG_NOSIGNAL) == 0)
            || send_fd_with_flags(control_fd, client_fd, initial, initial_len, MSG_NOSIGNAL) == 0)) {
            return 0;
        }
    }

    return -1;
}

static size_t read_initial(int client_fd, char *buffer, size_t buffer_len) {
    if (!lb_prebuffer_initial || buffer_len == 0) {
        return 0;
    }

    for (;;) {
        ssize_t received = recv(client_fd, buffer, buffer_len, MSG_DONTWAIT);
        if (received > 0) {
            return (size_t)received;
        }
        if (received == 0) {
            return 0;
        }
        if (errno == EINTR) {
            continue;
        }
        return 0;
    }
}

static void accept_clients(int listener, int accept_batch) {
    for (int accepted = 0; accepted < accept_batch; accepted++) {
        // Keep the client fd nonblocking across SCM_RIGHTS handoff; APIs do not redo fcntl.
        int client_fd = accept4(listener, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EINTR) {
                accepted--;
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            perror("accept4");
            return;
        }

        configure_client(client_fd);
        char initial[8192];
        size_t initial_len = read_initial(client_fd, initial, sizeof(initial));
        (void)deliver_fd(client_fd, initial, initial_len);
        close(client_fd);
    }
}

static int create_listener(int port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }

    int one = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
    if (env_enabled("TCP_DEFER_ACCEPT", 0)) {
        (void)setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &one, sizeof(one));
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, backlog) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static int fd_in_range(int fd) {
    return fd >= 0 && fd < MAX_FD_SLOTS;
}

static size_t buffered_len(size_t head, size_t tail) {
    return tail - head;
}

static void compact_buffer(char *buf, size_t *head, size_t *tail) {
    if (*head == 0) {
        return;
    }
    if (*head == *tail) {
        *head = 0;
        *tail = 0;
        return;
    }
    memmove(buf, buf + *head, *tail - *head);
    *tail -= *head;
    *head = 0;
}

static int epoll_add_fd(int epoll_fd, int fd, uint32_t events) {
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = events;
    event.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event);
}

static int epoll_mod_fd(int epoll_fd, int fd, uint32_t events) {
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = events;
    event.data.fd = fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &event);
}

static void close_proxy_conn(int epoll_fd, struct proxy_conn *conn) {
    if (conn == NULL) {
        return;
    }

    if (fd_in_range(conn->client_fd) && proxy_by_fd[conn->client_fd] == conn) {
        proxy_by_fd[conn->client_fd] = NULL;
    }
    if (fd_in_range(conn->backend_fd) && proxy_by_fd[conn->backend_fd] == conn) {
        proxy_by_fd[conn->backend_fd] = NULL;
    }
    if (conn->client_fd >= 0) {
        (void)epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->client_fd, NULL);
        close(conn->client_fd);
    }
    if (conn->backend_fd >= 0) {
        (void)epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->backend_fd, NULL);
        close(conn->backend_fd);
    }
    free(conn);
}

static int check_connect_done_raw(int backend_fd, int *connecting) {
    if (!*connecting) {
        return 0;
    }

    int err = 0;
    socklen_t len = sizeof(err);
    if (getsockopt(backend_fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        return -1;
    }
    if (err != 0) {
        errno = err;
        return -1;
    }
    *connecting = 0;
    return 0;
}

static int check_connect_done(struct proxy_conn *conn) {
    return check_connect_done_raw(conn->backend_fd, &conn->connecting);
}

static int read_into_buffer(int fd, char *buf, size_t *head, size_t *tail, int *eof) {
    compact_buffer(buf, head, tail);
    while (*tail < PROXY_BUF_SIZE) {
        ssize_t received = recv(fd, buf + *tail, PROXY_BUF_SIZE - *tail, 0);
        if (received > 0) {
            *tail += (size_t)received;
            continue;
        }
        if (received == 0) {
            *eof = 1;
            return 0;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        return -1;
    }
    return 0;
}

static int flush_buffer(int fd, char *buf, size_t *head, size_t *tail) {
    while (*head < *tail) {
        ssize_t sent = send(fd, buf + *head, *tail - *head, MSG_NOSIGNAL);
        if (sent > 0) {
            *head += (size_t)sent;
            continue;
        }
        if (sent == 0) {
            return -1;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        return -1;
    }

    if (*head == *tail) {
        *head = 0;
        *tail = 0;
    }
    return 0;
}

static int update_proxy_interest(int epoll_fd, struct proxy_conn *conn) {
    if (conn->client_eof || conn->backend_eof) {
        return -1;
    }

    uint32_t client_events = EPOLLRDHUP;
    uint32_t backend_events = EPOLLRDHUP;

    if (conn->c2b_tail < PROXY_BUF_SIZE) {
        client_events |= EPOLLIN;
    }
    if (buffered_len(conn->b2c_head, conn->b2c_tail) > 0) {
        client_events |= EPOLLOUT;
    }
    if (!conn->connecting && conn->b2c_tail < PROXY_BUF_SIZE) {
        backend_events |= EPOLLIN;
    }
    if (conn->connecting || buffered_len(conn->c2b_head, conn->c2b_tail) > 0) {
        backend_events |= EPOLLOUT;
    }

    if (epoll_mod_fd(epoll_fd, conn->client_fd, client_events) < 0) {
        return -1;
    }
    if (epoll_mod_fd(epoll_fd, conn->backend_fd, backend_events) < 0) {
        return -1;
    }
    return 0;
}

static int handle_proxy_event(int epoll_fd, struct proxy_conn *conn, int fd, uint32_t events) {
    if (fd == conn->backend_fd && (events & EPOLLOUT) && check_connect_done(conn) < 0) {
        return -1;
    }

    if (fd == conn->client_fd && (events & EPOLLIN)) {
        if (read_into_buffer(conn->client_fd, conn->c2b, &conn->c2b_head, &conn->c2b_tail, &conn->client_eof) < 0) {
            return -1;
        }
    }
    if (fd == conn->backend_fd && !conn->connecting && (events & EPOLLIN)) {
        if (read_into_buffer(conn->backend_fd, conn->b2c, &conn->b2c_head, &conn->b2c_tail, &conn->backend_eof) < 0) {
            return -1;
        }
    }

    if (!conn->connecting && buffered_len(conn->c2b_head, conn->c2b_tail) > 0) {
        if (flush_buffer(conn->backend_fd, conn->c2b, &conn->c2b_head, &conn->c2b_tail) < 0) {
            return -1;
        }
    }
    if (buffered_len(conn->b2c_head, conn->b2c_tail) > 0) {
        if (flush_buffer(conn->client_fd, conn->b2c, &conn->b2c_head, &conn->b2c_tail) < 0) {
            return -1;
        }
    }

    if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        if (fd == conn->client_fd) {
            conn->client_eof = 1;
        } else {
            conn->backend_eof = 1;
        }
    }

    return update_proxy_interest(epoll_fd, conn);
}

static void accept_proxy_clients(int listener, int epoll_fd, int accept_batch) {
    for (int accepted = 0; accepted < accept_batch; accepted++) {
        int client_fd = accept4(listener, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EINTR) {
                accepted--;
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            perror("accept4");
            return;
        }

        configure_client(client_fd);

        int connecting = 0;
        unsigned int backend = choose_backend();
        int backend_fd = connect_backend_stream(backend, &connecting);
        if (backend_fd < 0 || !fd_in_range(client_fd) || !fd_in_range(backend_fd)) {
            if (backend_fd >= 0) {
                close(backend_fd);
            }
            close(client_fd);
            continue;
        }

        struct proxy_conn *conn = calloc(1, sizeof(*conn));
        if (conn == NULL) {
            close(client_fd);
            close(backend_fd);
            continue;
        }
        conn->client_fd = client_fd;
        conn->backend_fd = backend_fd;
        conn->connecting = connecting;
        proxy_by_fd[client_fd] = conn;
        proxy_by_fd[backend_fd] = conn;

        if (epoll_add_fd(epoll_fd, client_fd, EPOLLIN | EPOLLRDHUP) < 0 ||
            epoll_add_fd(epoll_fd, backend_fd, (connecting ? EPOLLOUT : EPOLLIN) | EPOLLRDHUP) < 0) {
            close_proxy_conn(epoll_fd, conn);
            continue;
        }
    }
}

static int run_proxy_lb(int port, int backlog, int accept_batch) {
    int listener = create_listener(port, backlog);
    if (listener < 0) {
        perror("listen");
        return 1;
    }

    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        close(listener);
        return 1;
    }
    if (epoll_add_fd(epoll_fd, listener, EPOLLIN) < 0) {
        perror("epoll_ctl");
        close(listener);
        close(epoll_fd);
        return 1;
    }

    fprintf(stderr, "serving proxy load balancer on 0.0.0.0:%d backends=%u\n", port, backend_count);

    struct epoll_event events[MAX_EVENTS];
    for (;;) {
        int ready = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < ready; i++) {
            int fd = events[i].data.fd;
            if (fd == listener) {
                accept_proxy_clients(listener, epoll_fd, accept_batch);
                continue;
            }

            struct proxy_conn *conn = fd_in_range(fd) ? proxy_by_fd[fd] : NULL;
            if (conn == NULL || handle_proxy_event(epoll_fd, conn, fd, events[i].events) < 0) {
                close_proxy_conn(epoll_fd, conn);
            }
        }
    }

    close(listener);
    close(epoll_fd);
    return 1;
}

static int starts_with_len(const char *buf, size_t len, const char *prefix) {
    size_t prefix_len = strlen(prefix);
    return len >= prefix_len && memcmp(buf, prefix, prefix_len) == 0;
}

static int ascii_eq_ci(char left, char right) {
    if (left >= 'A' && left <= 'Z') {
        left = (char)(left + ('a' - 'A'));
    }
    if (right >= 'A' && right <= 'Z') {
        right = (char)(right + ('a' - 'A'));
    }
    return left == right;
}

static int header_key_eq_ci(const char *buf, const char *key) {
    for (size_t i = 0; key[i] != '\0'; i++) {
        if (!ascii_eq_ci(buf[i], key[i])) {
            return 0;
        }
    }
    return 1;
}

static int append_bytes(char *buf, size_t *tail, const char *data, size_t len) {
    if (*tail + len > PROXY_BUF_SIZE) {
        return -1;
    }
    memcpy(buf + *tail, data, len);
    *tail += len;
    return 0;
}

static const char *response_for_code(unsigned char code, size_t *len) {
    switch (code) {
        case 0:
            *len = sizeof(resp_approved_0) - 1;
            return resp_approved_0;
        case 1:
            *len = sizeof(resp_approved_02) - 1;
            return resp_approved_02;
        case 2:
            *len = sizeof(resp_approved_04) - 1;
            return resp_approved_04;
        case 3:
            *len = sizeof(resp_rejected_06) - 1;
            return resp_rejected_06;
        case 4:
            *len = sizeof(resp_rejected_08) - 1;
            return resp_rejected_08;
        case 5:
            *len = sizeof(resp_rejected_1) - 1;
            return resp_rejected_1;
        default:
            *len = sizeof(resp_approved_0) - 1;
            return resp_approved_0;
    }
}

static ssize_t find_header_end(const char *buf, size_t head, size_t tail) {
    for (size_t i = head; i + 3 < tail; i++) {
        if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n') {
            return (ssize_t)i;
        }
    }
    return -1;
}

static ssize_t find_line_end(const char *buf, size_t head, size_t header_end) {
    for (size_t i = head; i < header_end; i++) {
        if (buf[i] == '\r') {
            return (ssize_t)i;
        }
    }
    return -1;
}

static size_t parse_content_length_header(const char *buf, size_t start, size_t end) {
    static const char key[] = "content-length:";
    size_t key_len = sizeof(key) - 1;

    for (size_t i = start; i + key_len <= end; i++) {
        if (!header_key_eq_ci(buf + i, key)) {
            continue;
        }

        size_t pos = i + key_len;
        while (pos < end && (buf[pos] == ' ' || buf[pos] == '\t')) {
            pos++;
        }

        size_t value = 0;
        while (pos < end && buf[pos] >= '0' && buf[pos] <= '9') {
            value = value * 10 + (size_t)(buf[pos] - '0');
            pos++;
        }
        return value;
    }

    return 0;
}

static int consume_rpc_input(struct rpc_conn *conn) {
    compact_buffer(conn->in, &conn->in_head, &conn->in_tail);

    while (!conn->awaiting_response && conn->rpc_head == conn->rpc_tail && conn->in_head < conn->in_tail) {
        ssize_t header_end_signed = find_header_end(conn->in, conn->in_head, conn->in_tail);
        if (header_end_signed < 0) {
            return 0;
        }
        size_t header_end = (size_t)header_end_signed;
        ssize_t line_end_signed = find_line_end(conn->in, conn->in_head, header_end);
        if (line_end_signed < 0) {
            if (append_bytes(conn->out, &conn->out_tail, resp_bad_request, sizeof(resp_bad_request) - 1) < 0) {
                return -1;
            }
            conn->client_eof = 1;
            conn->in_head = conn->in_tail;
            return 0;
        }

        size_t line_end = (size_t)line_end_signed;
        const char *line = conn->in + conn->in_head;
        size_t line_len = line_end - conn->in_head;
        size_t consumed = header_end + 4 - conn->in_head;

        if (starts_with_len(line, line_len, "GET /ready")) {
            if (append_bytes(conn->out, &conn->out_tail, resp_ready, sizeof(resp_ready) - 1) < 0) {
                return -1;
            }
            conn->in_head += consumed;
            compact_buffer(conn->in, &conn->in_head, &conn->in_tail);
            continue;
        }

        if (!starts_with_len(line, line_len, "POST /fraud-score")) {
            if (append_bytes(conn->out, &conn->out_tail, resp_not_found, sizeof(resp_not_found) - 1) < 0) {
                return -1;
            }
            conn->in_head += consumed;
            compact_buffer(conn->in, &conn->in_head, &conn->in_tail);
            continue;
        }

        size_t body_len = parse_content_length_header(conn->in, line_end, header_end);
        size_t body_start = header_end + 4;
        size_t body_end = body_start + body_len;
        if (body_end < body_start || body_len > 65535 || body_len + 2 > PROXY_BUF_SIZE) {
            if (append_bytes(conn->out, &conn->out_tail, resp_bad_request, sizeof(resp_bad_request) - 1) < 0) {
                return -1;
            }
            conn->client_eof = 1;
            conn->in_head = conn->in_tail;
            return 0;
        }
        if (conn->in_tail < body_end) {
            return 0;
        }

        conn->rpc_head = 0;
        conn->rpc_tail = body_len + 2;
        conn->rpc[0] = (char)(body_len & 0xff);
        conn->rpc[1] = (char)((body_len >> 8) & 0xff);
        memcpy(conn->rpc + 2, conn->in + body_start, body_len);
        conn->awaiting_response = 1;
        conn->in_head = body_end;
        compact_buffer(conn->in, &conn->in_head, &conn->in_tail);
    }

    return 0;
}

static int update_rpc_interest(int epoll_fd, struct rpc_conn *conn) {
    if (conn->client_eof || conn->backend_eof) {
        return -1;
    }

    uint32_t client_events = EPOLLRDHUP;
    uint32_t backend_events = EPOLLRDHUP;

    if (conn->in_tail < PROXY_BUF_SIZE) {
        client_events |= EPOLLIN;
    }
    if (buffered_len(conn->out_head, conn->out_tail) > 0) {
        client_events |= EPOLLOUT;
    }
    if (!conn->connecting && conn->awaiting_response && conn->out_tail < PROXY_BUF_SIZE) {
        backend_events |= EPOLLIN;
    }
    if (conn->connecting || buffered_len(conn->rpc_head, conn->rpc_tail) > 0) {
        backend_events |= EPOLLOUT;
    }

    if (epoll_mod_fd(epoll_fd, conn->client_fd, client_events) < 0) {
        return -1;
    }
    if (epoll_mod_fd(epoll_fd, conn->backend_fd, backend_events) < 0) {
        return -1;
    }
    return 0;
}

static void close_rpc_conn(int epoll_fd, struct rpc_conn *conn) {
    if (conn == NULL) {
        return;
    }

    if (fd_in_range(conn->client_fd) && rpc_by_fd[conn->client_fd] == conn) {
        rpc_by_fd[conn->client_fd] = NULL;
    }
    if (fd_in_range(conn->backend_fd) && rpc_by_fd[conn->backend_fd] == conn) {
        rpc_by_fd[conn->backend_fd] = NULL;
    }
    if (conn->client_fd >= 0) {
        (void)epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->client_fd, NULL);
        close(conn->client_fd);
    }
    if (conn->backend_fd >= 0) {
        (void)epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->backend_fd, NULL);
        close(conn->backend_fd);
    }
    free(conn);
}

static int read_rpc_response(struct rpc_conn *conn) {
    while (conn->awaiting_response && conn->out_tail + sizeof(resp_rejected_1) < PROXY_BUF_SIZE) {
        unsigned char code = 0;
        ssize_t received = recv(conn->backend_fd, &code, 1, 0);
        if (received > 0) {
            size_t len = 0;
            const char *response = response_for_code(code, &len);
            if (append_bytes(conn->out, &conn->out_tail, response, len) < 0) {
                return -1;
            }
            conn->awaiting_response = 0;
            if (consume_rpc_input(conn) < 0) {
                return -1;
            }
            continue;
        }
        if (received == 0) {
            conn->backend_eof = 1;
            return 0;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        return -1;
    }
    return 0;
}

static int handle_rpc_event(int epoll_fd, struct rpc_conn *conn, int fd, uint32_t events) {
    if (fd == conn->backend_fd && (events & EPOLLOUT) && check_connect_done_raw(conn->backend_fd, &conn->connecting) < 0) {
        return -1;
    }

    if (fd == conn->client_fd && (events & EPOLLIN)) {
        if (read_into_buffer(conn->client_fd, conn->in, &conn->in_head, &conn->in_tail, &conn->client_eof) < 0) {
            return -1;
        }
        if (consume_rpc_input(conn) < 0) {
            return -1;
        }
    }
    if (fd == conn->backend_fd && !conn->connecting && (events & EPOLLIN)) {
        if (read_rpc_response(conn) < 0) {
            return -1;
        }
    }

    if (!conn->connecting && buffered_len(conn->rpc_head, conn->rpc_tail) > 0) {
        if (flush_buffer(conn->backend_fd, conn->rpc, &conn->rpc_head, &conn->rpc_tail) < 0) {
            return -1;
        }
    }
    if (buffered_len(conn->out_head, conn->out_tail) > 0) {
        if (flush_buffer(conn->client_fd, conn->out, &conn->out_head, &conn->out_tail) < 0) {
            return -1;
        }
    }

    if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        if (fd == conn->client_fd) {
            conn->client_eof = 1;
        } else {
            conn->backend_eof = 1;
        }
    }

    return update_rpc_interest(epoll_fd, conn);
}

static void accept_rpc_clients(int listener, int epoll_fd, int accept_batch) {
    for (int accepted = 0; accepted < accept_batch; accepted++) {
        int client_fd = accept4(listener, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EINTR) {
                accepted--;
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            perror("accept4");
            return;
        }

        configure_client(client_fd);

        int connecting = 0;
        unsigned int backend = choose_backend();
        int backend_fd = connect_backend_stream(backend, &connecting);
        if (backend_fd < 0 || !fd_in_range(client_fd) || !fd_in_range(backend_fd)) {
            if (backend_fd >= 0) {
                close(backend_fd);
            }
            close(client_fd);
            continue;
        }

        struct rpc_conn *conn = calloc(1, sizeof(*conn));
        if (conn == NULL) {
            close(client_fd);
            close(backend_fd);
            continue;
        }
        conn->client_fd = client_fd;
        conn->backend_fd = backend_fd;
        conn->connecting = connecting;
        rpc_by_fd[client_fd] = conn;
        rpc_by_fd[backend_fd] = conn;

        if (epoll_add_fd(epoll_fd, client_fd, EPOLLIN | EPOLLRDHUP) < 0 ||
            epoll_add_fd(epoll_fd, backend_fd, (connecting ? EPOLLOUT : EPOLLIN) | EPOLLRDHUP) < 0) {
            close_rpc_conn(epoll_fd, conn);
            continue;
        }
    }
}

static int run_rpc_lb(int port, int backlog, int accept_batch) {
    int listener = create_listener(port, backlog);
    if (listener < 0) {
        perror("listen");
        return 1;
    }

    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        close(listener);
        return 1;
    }
    if (epoll_add_fd(epoll_fd, listener, EPOLLIN) < 0) {
        perror("epoll_ctl");
        close(listener);
        close(epoll_fd);
        return 1;
    }

    fprintf(stderr, "serving rpc load balancer on 0.0.0.0:%d backends=%u\n", port, backend_count);

    struct epoll_event events[MAX_EVENTS];
    for (;;) {
        int ready = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < ready; i++) {
            int fd = events[i].data.fd;
            if (fd == listener) {
                accept_rpc_clients(listener, epoll_fd, accept_batch);
                continue;
            }

            struct rpc_conn *conn = fd_in_range(fd) ? rpc_by_fd[fd] : NULL;
            if (conn == NULL || handle_rpc_event(epoll_fd, conn, fd, events[i].events) < 0) {
                close_rpc_conn(epoll_fd, conn);
            }
        }
    }

    close(listener);
    close(epoll_fd);
    return 1;
}

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    init_control_fds();
    init_backends();

    fd_control_seqpacket = env_enabled("FD_CONTROL_SEQPACKET", 1);
    lb_preconnect_control = env_enabled("LB_PRECONNECT_CONTROL", 1);
    lb_tcp_nodelay = env_enabled("LB_TCP_NODELAY", 1);
    lb_socket_buffers = env_enabled("LB_SOCKET_BUFFERS", 1);
    lb_sendmsg_dontwait = env_enabled("LB_SENDMSG_DONTWAIT", 1);
    lb_prebuffer_initial = env_enabled("LB_PREBUFFER_INITIAL", 0);
    socket_buffer_size = env_int("SOCKET_BUFFER_SIZE", 16384);

    int port = env_int("LB_PORT", 9999);
    int backlog = env_int("LB_BACKLOG", 65535);
    int accept_batch = env_int("LB_ACCEPT_BATCH", 128);
    const char *mode = getenv("LB_MODE");
    if (mode != NULL && strcmp(mode, "proxy") == 0) {
        return run_proxy_lb(port, backlog, accept_batch);
    }
    if (mode != NULL && strcmp(mode, "rpc") == 0) {
        return run_rpc_lb(port, backlog, accept_batch);
    }

    int listener = create_listener(port, backlog);
    if (listener < 0) {
        perror("listen");
        return 1;
    }

    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        close(listener);
        return 1;
    }

    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = listener;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listener, &event) < 0) {
        perror("epoll_ctl");
        close(listener);
        close(epoll_fd);
        return 1;
    }

    preconnect_controls();
    fprintf(stderr, "serving fdpass load balancer on 0.0.0.0:%d backends=%u\n", port, backend_count);

    struct epoll_event events[MAX_EVENTS];
    for (;;) {
        int ready = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < ready; i++) {
            if (events[i].data.fd == listener) {
                accept_clients(listener, accept_batch);
            }
        }
    }

    close(listener);
    close(epoll_fd);
    for (unsigned int i = 0; i < backend_count; i++) {
        if (control_fds[i] >= 0) {
            close(control_fds[i]);
        }
    }

    return 1;
}
