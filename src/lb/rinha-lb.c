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
static int socket_buffer_size = 16384;

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
    }
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

static int ensure_control(unsigned int index) {
    if (control_fds[index] >= 0) {
        return control_fds[index];
    }

    int fd = connect_control(index);
    control_fds[index] = fd;
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

static int send_fd(int control_fd, int fd_to_send) {
    char data = 0;
    struct iovec io;
    io.iov_base = &data;
    io.iov_len = 1;

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
        ssize_t sent = sendmsg(control_fd, &msg, MSG_NOSIGNAL);
        if (sent == 1) {
            return 0;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        return -1;
    }
}

static int deliver_fd(int client_fd) {
    unsigned int start = choose_backend();
    for (unsigned int attempt = 0; attempt < backend_count; attempt++) {
        unsigned int index = (start + attempt) % backend_count;
        int control_fd = ensure_control(index);
        if (control_fd >= 0 && send_fd(control_fd, client_fd) == 0) {
            return 0;
        }

        if (control_fds[index] >= 0) {
            close(control_fds[index]);
            control_fds[index] = -1;
        }
    }

    return -1;
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
        (void)deliver_fd(client_fd);
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

int main(void) {
    signal(SIGPIPE, SIG_IGN);
    init_control_fds();
    init_backends();

    fd_control_seqpacket = env_enabled("FD_CONTROL_SEQPACKET", 0);
    lb_preconnect_control = env_enabled("LB_PRECONNECT_CONTROL", 1);
    lb_tcp_nodelay = env_enabled("LB_TCP_NODELAY", 1);
    lb_socket_buffers = env_enabled("LB_SOCKET_BUFFERS", 1);
    socket_buffer_size = env_int("SOCKET_BUFFER_SIZE", 16384);

    int port = env_int("LB_PORT", 9999);
    int backlog = env_int("LB_BACKLOG", 65535);
    int accept_batch = env_int("LB_ACCEPT_BATCH", 128);
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
