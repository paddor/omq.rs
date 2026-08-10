#ifndef OMQ_ZGUIDE_ZG_H
#define OMQ_ZGUIDE_ZG_H

#define _POSIX_C_SOURCE 200809L

#include "zmq.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define ZG_MAX_PARTS 16

typedef struct {
    int count;
    char *data[ZG_MAX_PARTS];
    size_t size[ZG_MAX_PARTS];
} zg_msg;

static inline void zg_die(const char *what) {
    fprintf(stderr, "%s: %s\n", what, zmq_strerror(zmq_errno()));
    exit(1);
}

static inline void zg_check(int rc, const char *what) {
    if (rc < 0) {
        zg_die(what);
    }
}

static inline char *zg_strdup(const char *s) {
    size_t n = strlen(s);
    char *out = (char *)malloc(n + 1);
    if (out == NULL) {
        perror("malloc");
        exit(1);
    }
    memcpy(out, s, n + 1);
    return out;
}

static inline char *zg_strndup(const void *data, size_t len) {
    char *out = (char *)malloc(len + 1);
    if (out == NULL) {
        perror("malloc");
        exit(1);
    }
    memcpy(out, data, len);
    out[len] = 0;
    return out;
}

static inline void zg_sleep_ms(int ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

static inline int64_t zg_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static inline const char *zg_arg(int argc, char **argv, int index, const char *fallback) {
    return argc > index ? argv[index] : fallback;
}

static inline int zg_arg_int(int argc, char **argv, int index, int fallback) {
    return argc > index ? atoi(argv[index]) : fallback;
}

static inline double zg_arg_double(int argc, char **argv, int index, double fallback) {
    return argc > index ? atof(argv[index]) : fallback;
}

static inline void zg_set_i32(void *socket, int option, int value) {
    zg_check(zmq_setsockopt(socket, option, &value, sizeof(value)), "zmq_setsockopt");
}

static inline void zg_subscribe(void *socket, const char *prefix) {
    zg_check(zmq_setsockopt(socket, ZMQ_SUBSCRIBE, prefix, strlen(prefix)), "ZMQ_SUBSCRIBE");
}

static inline void *zg_ctx(void) {
    void *ctx = zmq_ctx_new();
    if (ctx == NULL) {
        zg_die("zmq_ctx_new");
    }
    return ctx;
}

static inline void *zg_socket(void *ctx, int type) {
    void *socket = zmq_socket(ctx, type);
    if (socket == NULL) {
        zg_die("zmq_socket");
    }
    int linger = 0;
    zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(linger));
    return socket;
}

static inline void zg_bind(void *socket, const char *endpoint) {
    if (zmq_bind(socket, endpoint) != 0) {
        fprintf(stderr, "bind %s failed\n", endpoint);
        zg_die("zmq_bind");
    }
}

static inline void zg_connect(void *socket, const char *endpoint) {
    if (zmq_connect(socket, endpoint) != 0) {
        fprintf(stderr, "connect %s failed\n", endpoint);
        zg_die("zmq_connect");
    }
}

static inline void zg_close(void *socket) {
    if (socket != NULL) {
        zmq_close(socket);
    }
}

static inline void zg_term(void *ctx) {
    if (ctx != NULL) {
        zmq_ctx_term(ctx);
    }
}

static inline void zg_msg_init(zg_msg *msg) {
    memset(msg, 0, sizeof(*msg));
}

static inline void zg_msg_clear(zg_msg *msg) {
    for (int i = 0; i < msg->count; i++) {
        free(msg->data[i]);
        msg->data[i] = NULL;
        msg->size[i] = 0;
    }
    msg->count = 0;
}

static inline int zg_msg_recv(void *socket, zg_msg *out) {
    zg_msg_clear(out);

    int more = 0;
    do {
        if (out->count == ZG_MAX_PARTS) {
            fprintf(stderr, "too many message parts\n");
            return -1;
        }

        zmq_msg_t part;
        zg_check(zmq_msg_init(&part), "zmq_msg_init");
        int n = zmq_msg_recv(&part, socket, 0);
        if (n < 0) {
            zmq_msg_close(&part);
            return -1;
        }

        size_t len = zmq_msg_size(&part);
        out->data[out->count] = zg_strndup(zmq_msg_data(&part), len);
        out->size[out->count] = len;
        out->count++;
        zmq_msg_close(&part);

        size_t more_size = sizeof(more);
        zg_check(zmq_getsockopt(socket, ZMQ_RCVMORE, &more, &more_size), "ZMQ_RCVMORE");
    } while (more);

    return out->count;
}

static inline void zg_msg_send(void *socket, const zg_msg *msg) {
    for (int i = 0; i < msg->count; i++) {
        int flags = i + 1 == msg->count ? 0 : ZMQ_SNDMORE;
        zg_check(zmq_send(socket, msg->data[i], msg->size[i], flags), "zmq_send");
    }
}

static inline void zg_send_str(void *socket, const char *data) {
    zg_check(zmq_send(socket, data, strlen(data), 0), "zmq_send");
}

static inline void zg_send_more(void *socket, const char *data) {
    zg_check(zmq_send(socket, data, strlen(data), ZMQ_SNDMORE), "zmq_send");
}

static inline void zg_send_data(void *socket, const void *data, size_t len, int more) {
    zg_check(zmq_send(socket, data, len, more ? ZMQ_SNDMORE : 0), "zmq_send");
}

static inline char *zg_recv_str(void *socket) {
    zg_msg msg;
    zg_msg_init(&msg);
    if (zg_msg_recv(socket, &msg) < 0) {
        return NULL;
    }
    char *out = msg.count > 0 ? zg_strdup(msg.data[msg.count - 1]) : zg_strdup("");
    zg_msg_clear(&msg);
    return out;
}

static inline char *zg_recv_str_timeout(void *socket, int timeout_ms) {
    zg_set_i32(socket, ZMQ_RCVTIMEO, timeout_ms);
    char *out = zg_recv_str(socket);
    if (out == NULL && zmq_errno() != EAGAIN) {
        zg_die("zmq_recv");
    }
    return out;
}

static inline char *zg_printf_alloc(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    va_list copy;
    va_copy(copy, ap);
    int n = vsnprintf(NULL, 0, fmt, copy);
    va_end(copy);
    if (n < 0) {
        perror("vsnprintf");
        exit(1);
    }
    char *buf = (char *)malloc((size_t)n + 1);
    if (buf == NULL) {
        perror("malloc");
        exit(1);
    }
    vsnprintf(buf, (size_t)n + 1, fmt, ap);
    va_end(ap);
    return buf;
}

static inline void zg_upper(char *s) {
    for (; *s != 0; s++) {
        if (*s >= 'a' && *s <= 'z') {
            *s = (char)(*s - ('a' - 'A'));
        }
    }
}

#endif
