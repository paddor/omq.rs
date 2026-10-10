/*
 * Two-process throughput peer for libzmq 4.x.
 *
 * Usage:
 *   omq_libzmq_baseline_peer push <addr> <msg_size_bytes>
 *   omq_libzmq_baseline_peer pull <addr> <msg_size_bytes> <duration_secs>
 *   omq_libzmq_baseline_peer rep  <addr> <msg_size_bytes>
 *   omq_libzmq_baseline_peer req  <addr> <msg_size_bytes> <iterations> <warmup>
 *
 * <addr>: a port number (→ tcp://127.0.0.1:<port>) or a full ZMQ address
 *         (e.g. ipc:///tmp/bench.sock or tcp://127.0.0.1:15555).
 *
 * Push: binds, sends <msg_size> byte messages forever.
 * Pull: connects, warms up for 500 ms, then counts for <duration> seconds.
 * Rep:  binds, echoes received messages back forever.
 * Req:  connects, runs warmup + measured round-trips, prints latency
 *       percentiles (p50 p99 p999 max iterations) in microseconds.
 *
 * Compile: gcc -O2 -o omq_libzmq_baseline_peer libzmq_bench_peer.c -lzmq
 *
 * Output (pull only, one line to stdout):
 *   <count> <elapsed_secs> <msg_size> <cpu_secs>
 */

#define _DEFAULT_SOURCE
#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/resource.h>
#include <sys/wait.h>

static double now_secs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static double wall_secs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void wait_for_start_barrier(void) {
    const char *s = getenv("OMQ_BENCH_START_AT");
    if (!s || !*s) return;
    double start_at = atof(s);
    for (;;) {
        double remaining = start_at - wall_secs();
        if (remaining <= 0) return;
        struct timespec ts;
        ts.tv_sec = (time_t)remaining;
        ts.tv_nsec = (long)((remaining - ts.tv_sec) * 1e9);
        nanosleep(&ts, NULL);
    }
}

static double cpu_time_secs(void) {
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    return (ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6)
         + (ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6);
}

static void die(const char *msg) {
    fprintf(stderr, "%s: %s\n", msg, zmq_strerror(zmq_errno()));
    exit(1);
}

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static double quantile_d(const double *sorted, int n, double p) {
    double idx = (n - 1) * p;
    return sorted[(int)(idx + 0.5)];
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t *)a;
    uint64_t vb = *(const uint64_t *)b;
    return (va > vb) - (va < vb);
}

static double percentile_u64(uint64_t *sorted, int n, double p) {
    int idx = (int)(n * p / 100.0);
    if (idx >= n) idx = n - 1;
    return sorted[idx] / 1000.0;
}

static void print_fairness(const double *rates, int n, double elapsed) {
    double sorted[256];
    for (int i = 0; i < n; i++)
        sorted[i] = rates[i];
    qsort(sorted, n, sizeof(double), cmp_double);
    printf(" %.1f %.1f %.1f %.1f %.1f",
           quantile_d(sorted, n, 0.10),
           quantile_d(sorted, n, 0.25),
           quantile_d(sorted, n, 0.50),
           quantile_d(sorted, n, 0.75),
           quantile_d(sorted, n, 0.90));
    for (int i = 0; i < n; i++)
        printf(" %.1f", sorted[i]);
    printf("\n");
    (void)elapsed;
}

/* Fixed CURVE keypairs for benchmarking (Z85-encoded, 40 chars each). */
#define CURVE_SERVER_PUBLIC "c[nMZc{RbXmBJGbya&4/kW#Y.Ql4uT1wNJmp6/D@"
#define CURVE_SERVER_SECRET "2(&.P[s%^&[zcwc3wjPJHlsb^j.F]{F4Bh{ed8S?"
#define CURVE_CLIENT_PUBLIC "c[m^Q0-jh2zf931U<A)s8LsJaZ>KZY1#Na%Plr<s"
#define CURVE_CLIENT_SECRET "R0/+YXMg3!(j7@pEBeg:r4hj]Nuldfd7-&jIGw+/"

static int bench_curve_enabled(void) {
    const char *s = getenv("ZMQ_BENCH_CURVE");
    return s && *s && strcmp(s, "0") != 0;
}

static void setup_curve_server(void *sock) {
    int as_server = 1;
    zmq_setsockopt(sock, ZMQ_CURVE_SERVER, &as_server, sizeof(as_server));
    zmq_setsockopt(sock, ZMQ_CURVE_PUBLICKEY, CURVE_SERVER_PUBLIC, 40);
    zmq_setsockopt(sock, ZMQ_CURVE_SECRETKEY, CURVE_SERVER_SECRET, 40);
}

static void setup_curve_client(void *sock) {
    zmq_setsockopt(sock, ZMQ_CURVE_PUBLICKEY, CURVE_CLIENT_PUBLIC, 40);
    zmq_setsockopt(sock, ZMQ_CURVE_SECRETKEY, CURVE_CLIENT_SECRET, 40);
    zmq_setsockopt(sock, ZMQ_CURVE_SERVERKEY, CURVE_SERVER_PUBLIC, 40);
}

static void report_bound_port(void *ctx, void *sock) {
    char ep[256];
    size_t ep_len = sizeof(ep);
    if (zmq_getsockopt(sock, ZMQ_LAST_ENDPOINT, ep, &ep_len) != 0)
        return;
    char *colon = strrchr(ep, ':');
    if (!colon)
        return;
    const char *port_str = colon + 1;

    const char *coord = getenv("OMQ_BENCH_COORD");
    if (!coord || !*coord)
        return;

    void *push = zmq_socket(ctx, ZMQ_PUSH);
    if (!push) return;
    int linger = 0;
    zmq_setsockopt(push, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_connect(push, coord);
    char msg[64];
    snprintf(msg, sizeof(msg), "READY %s", port_str);
    zmq_send(push, msg, strlen(msg), 0);
}

/* Returns a zmq address string. If s looks like a bare port number, expands
 * it to tcp://127.0.0.1:<port>. Otherwise returns s unchanged (caller must
 * not free the returned pointer if it equals s). */
static const char *resolve_addr(const char *s, char *buf, size_t bufsz) {
    int all_digits = 1;
    for (const char *p = s; *p; p++) {
        if (!isdigit((unsigned char)*p)) { all_digits = 0; break; }
    }
    if (all_digits && s[0] != '\0') {
        snprintf(buf, bufsz, "tcp://127.0.0.1:%s", s);
        return buf;
    }
    return s;
}

/* Stable socket pairs only. Draft types require a draft-enabled libzmq build. */
static int latency_socket_type(int requester) {
    const char *pair = getenv("OMQ_BENCH_LATENCY_PAIR");
    if (!pair || strcmp(pair, "req-rep") == 0)
        return requester ? ZMQ_REQ : ZMQ_REP;
    if (strcmp(pair, "router-dealer") == 0)
        return requester ? ZMQ_DEALER : ZMQ_ROUTER;
    if (strcmp(pair, "router-router") == 0) return ZMQ_ROUTER;
    if (strcmp(pair, "pair") == 0) return ZMQ_PAIR;
    fprintf(stderr, "unsupported libzmq latency pair: %s\n", pair);
    exit(1);
}

#define LATENCY_REQUESTER "bench-requester"
#define LATENCY_RESPONDER "bench-responder"

static void *latency_socket(void *ctx, int requester) {
    int type = latency_socket_type(requester);
    void *sock = zmq_socket(ctx, type);
    if (!sock) die("zmq_socket latency");
    const char *identity = requester ? LATENCY_REQUESTER : LATENCY_RESPONDER;
    if (zmq_setsockopt(sock, ZMQ_IDENTITY, identity, strlen(identity)) != 0)
        die("zmq_setsockopt IDENTITY");
    if (type == ZMQ_ROUTER) {
        int mandatory = 1;
        if (zmq_setsockopt(sock, ZMQ_ROUTER_MANDATORY, &mandatory, sizeof(mandatory)) != 0)
            die("zmq_setsockopt ROUTER_MANDATORY");
    }
    return sock;
}

static void validate_latency_message(zmq_msg_t *body, zmq_msg_t *identity,
                                     int routed, int requester, int size) {
    if (zmq_msg_size(body) != (size_t)size || zmq_msg_more(body)) {
        fprintf(stderr, "invalid latency body size or frame count\n");
        exit(1);
    }
    const unsigned char *bytes = zmq_msg_data(body);
    for (int i = 0; i < size; i++) {
        if (bytes[i] != 'x') {
            fprintf(stderr, "invalid latency body content\n");
            exit(1);
        }
    }
    if (routed) {
        const char *expected = requester ? LATENCY_RESPONDER : LATENCY_REQUESTER;
        if (!zmq_msg_more(identity) || zmq_msg_size(identity) != strlen(expected)
            || memcmp(zmq_msg_data(identity), expected, strlen(expected)) != 0) {
            fprintf(stderr, "invalid latency routing envelope\n");
            exit(1);
        }
    }
}

static void latency_echo(void *sock, int size) {
    int routed = latency_socket_type(0) == ZMQ_ROUTER;
    zmq_msg_t identity, body;
    zmq_msg_init(&identity);
    zmq_msg_init(&body);
    int first = 1;
    for (;;) {
        if (routed && zmq_msg_recv(&identity, sock, 0) < 0)
            die("zmq_recv latency identity");
        if (zmq_msg_recv(&body, sock, 0) < 0) die("zmq_recv latency body");
        if (first) {
            validate_latency_message(&body, &identity, routed, 0, size);
            first = 0;
        }
        if (routed && zmq_send(sock, zmq_msg_data(&identity), zmq_msg_size(&identity), ZMQ_SNDMORE) < 0)
            die("zmq_send latency identity");
        if (zmq_send(sock, zmq_msg_data(&body), zmq_msg_size(&body), 0) < 0)
            die("zmq_send latency body");
    }
}

static void latency_exchange(void *sock, const char *buf, int size, int routed,
                             zmq_msg_t *identity, zmq_msg_t *reply) {
    if (routed && zmq_send(sock, LATENCY_RESPONDER, strlen(LATENCY_RESPONDER), ZMQ_SNDMORE) < 0)
        die("zmq_send latency identity");
    if (zmq_send(sock, buf, size, 0) < 0) die("zmq_send latency body");
    if (routed && zmq_msg_recv(identity, sock, 0) < 0)
        die("zmq_recv latency identity");
    if (zmq_msg_recv(reply, sock, 0) < 0) die("zmq_recv latency body");
}


typedef struct { void *ctx; const char *name; int size; } InprocPushArg;
typedef struct { void *ctx; const char *name; int size; } InprocRepArg;
typedef struct { void *sock; int size; } InprocPubSendArg;

typedef struct {
    void *sock;
    _Atomic long long counter;
    volatile int stop;
} RecvWorker;

typedef struct {
    void *sock;
    int size;
    _Atomic long long counter;
    volatile int stop;
} SendWorker;

static void *inproc_push_thread(void *arg_) {
    InprocPushArg *a = arg_;
    char addr[256];
    snprintf(addr, sizeof(addr), "inproc://%s", a->name);
    void *sock = zmq_socket(a->ctx, ZMQ_PUSH);
    if (!sock || zmq_bind(sock, addr) != 0) return NULL;
    char *buf = calloc(1, a->size);
    memset(buf, 'x', a->size);
    for (;;) {
        if (zmq_send(sock, buf, a->size, 0) < 0) break;
    }
    free(buf);
    zmq_close(sock);
    return NULL;
}

static void *inproc_rep_thread(void *arg_) {
    InprocRepArg *a = arg_;
    char addr[256];
    snprintf(addr, sizeof(addr), "inproc://%s", a->name);
    void *sock = latency_socket(a->ctx, 0);
    if (zmq_bind(sock, addr) != 0) die("zmq_bind latency");
    latency_echo(sock, a->size);
    zmq_close(sock);
    return NULL;
}

static void *inproc_pub_send_thread(void *arg_) {
    InprocPubSendArg *a = arg_;
    char *buf = calloc(1, a->size);
    memset(buf, 'x', a->size);
    for (;;) {
        if (zmq_send(a->sock, buf, a->size, 0) < 0) break;
    }
    free(buf);
    return NULL;
}

static void *recv_thread(void *arg) {
    RecvWorker *w = arg;
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    while (!w->stop) {
        int rc = zmq_msg_recv(&msg, w->sock, 0);
        if (rc < 0) {
            if (zmq_errno() == EAGAIN) continue;
            break;
        }
        atomic_fetch_add_explicit(&w->counter, 1, memory_order_relaxed);
    }
    zmq_msg_close(&msg);
    return NULL;
}

static void *send_thread(void *arg) {
    SendWorker *w = arg;
    char *buf = calloc(1, w->size);
    memset(buf, 'x', w->size);
    while (!w->stop) {
        if (zmq_send(w->sock, buf, w->size, 0) < 0) {
            if (zmq_errno() == EAGAIN) continue;
            break;
        }
        atomic_fetch_add_explicit(&w->counter, 1, memory_order_relaxed);
    }
    free(buf);
    return NULL;
}

static void drain_subscribers(void **sockets, int count, int size,
                              double deadline, long long *counts) {
    zmq_pollitem_t items[256] = {{0}};
    for (int i = 0; i < count; i++) {
        items[i].socket = sockets[i];
        items[i].events = ZMQ_POLLIN;
    }
    int batch = size <= 1024 ? 64 : 256;
    int byte_limit = 65536 / (size > 0 ? size : 1);
    if (byte_limit < 1) byte_limit = 1;
    if (batch > byte_limit) batch = byte_limit;
    zmq_msg_t msg;
    if (zmq_msg_init(&msg) != 0) die("zmq_msg_init");
    while (now_secs() < deadline) {
        int received = 0;
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < batch; j++) {
                if (zmq_msg_recv(&msg, sockets[i], ZMQ_DONTWAIT) < 0) {
                    if (zmq_errno() == EAGAIN) break;
                    if (zmq_errno() == EINTR) continue;
                    die("multi-sub recv");
                }
                counts[i]++;
                received = 1;
            }
        }
        if (!received) {
            double remaining_ms = (deadline - now_secs()) * 1000.0;
            if (remaining_ms <= 0) break;
            long timeout_ms = (long)remaining_ms + 1;
            if (zmq_poll(items, count, timeout_ms) < 0 && zmq_errno() != EINTR)
                die("multi-sub poll");
        }
    }
    zmq_msg_close(&msg);
}

static void run_multi_sub(void *ctx, const char *addr, int size,
                          double duration, int count) {
    void *sockets[256];
    long long counts[256] = {0};
    for (int i = 0; i < count; i++) {
        sockets[i] = zmq_socket(ctx, ZMQ_SUB);
        if (!sockets[i]) die("zmq_socket SUB");
        if (bench_curve_enabled()) setup_curve_client(sockets[i]);
        if (zmq_setsockopt(sockets[i], ZMQ_SUBSCRIBE, "", 0) != 0)
            die("subscribe");
        if (zmq_connect(sockets[i], addr) != 0) die("zmq_connect");
    }
    const char *warmup_env = getenv("OMQ_BENCH_WARMUP_MS");
    double warmup = warmup_env ? atof(warmup_env) / 1000.0 : 0.5;
    const char *start_env = getenv("OMQ_BENCH_START_AT");
    if (start_env && *start_env) {
        double remaining = atof(start_env) - warmup - wall_secs();
        if (remaining > 0) {
            struct timespec ts = {
                (time_t)remaining,
                (long)((remaining - (time_t)remaining) * 1e9)
            };
            nanosleep(&ts, NULL);
        }
    }
    drain_subscribers(sockets, count, size, now_secs() + warmup, counts);
    wait_for_start_barrier();
    memset(counts, 0, sizeof(counts));
    double cpu_before = cpu_time_secs();
    double t0 = now_secs();
    drain_subscribers(sockets, count, size, t0 + duration, counts);
    double elapsed = now_secs() - t0;
    double cpu = cpu_time_secs() - cpu_before;
    long long total = 0;
    double rates[256];
    double per_min = 1e18, per_max = 0;
    for (int i = 0; i < count; i++) {
        total += counts[i];
        rates[i] = (double)counts[i] / elapsed;
        if (rates[i] < per_min) per_min = rates[i];
        if (rates[i] > per_max) per_max = rates[i];
    }
    printf("%lld %.6f %d %.6f %d %.1f %.1f",
           total, elapsed, size, cpu, count, per_min, per_max);
    print_fairness(rates, count, elapsed);
    for (int i = 0; i < count; i++) zmq_close(sockets[i]);
}

int main(int argc, char **argv) {
    if (argc < 4) goto usage;

    const char *role = argv[1];
    char addr_buf[256];
    const char *addr = resolve_addr(argv[2], addr_buf, sizeof(addr_buf));
    int size = atoi(argv[3]);

    void *ctx = zmq_ctx_new();
    if (!ctx) die("zmq_ctx_new");

    const char *io_threads_env = getenv("ZMQ_IO_THREADS");
    if (io_threads_env) {
        int n = atoi(io_threads_env);
        if (n > 0)
            zmq_ctx_set(ctx, ZMQ_IO_THREADS, n);
    }

    if (strcmp(role, "push") == 0) {
        void *sock = zmq_socket(ctx, ZMQ_PUSH);
        if (!sock) die("zmq_socket PUSH");
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");
        report_bound_port(ctx, sock);
        wait_for_start_barrier();

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        for (;;) {
            if (zmq_send(sock, buf, size, 0) < 0) break;
        }
        free(buf);
        zmq_close(sock);

    } else if (strcmp(role, "pull") == 0) {
        if (argc < 5) goto usage;
        double duration = atof(argv[4]);

        void *sock = zmq_socket(ctx, ZMQ_PULL);
        if (!sock) die("zmq_socket PULL");
        if (zmq_connect(sock, addr) != 0) die("zmq_connect");
        wait_for_start_barrier();

        zmq_msg_t msg;
        zmq_msg_init(&msg);

        double warmup_end = now_secs() + 0.5;
        while (now_secs() < warmup_end) {
            int rc = zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT);
            if (rc < 0) {
                struct timespec ts = {0, 100000};
                nanosleep(&ts, NULL);
            }
        }

        long long count = 0;
        double cpu_before = cpu_time_secs();
        double t0 = now_secs();
        double deadline = t0 + duration;

        zmq_pollitem_t items[1];
        items[0].socket = sock;
        items[0].events = ZMQ_POLLIN;

        for (;;) {
            double remaining = deadline - now_secs();
            if (remaining <= 0) break;
            long timeout_ms = (long)(remaining * 1000.0);
            if (timeout_ms < 1) timeout_ms = 1;
            int rc = zmq_poll(items, 1, timeout_ms);
            if (rc < 0) break;
            if (rc == 0) break;
            if (items[0].revents & ZMQ_POLLIN) {
                while (zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT) >= 0) {
                    count++;
                    if (now_secs() >= deadline) goto done;
                }
            }
        }
done:;
        double elapsed = now_secs() - t0;
        double cpu = cpu_time_secs() - cpu_before;
        printf("%lld %.6f %d %.6f\n", count, elapsed, size, cpu);

        zmq_msg_close(&msg);
        zmq_close(sock);

    } else if (strcmp(role, "push-connect") == 0) {
        void *sock = zmq_socket(ctx, ZMQ_PUSH);
        if (!sock) die("zmq_socket PUSH");
        if (zmq_connect(sock, addr) != 0) die("zmq_connect");
        wait_for_start_barrier();

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        for (;;) {
            if (zmq_send(sock, buf, size, 0) < 0) break;
        }
        free(buf);
        zmq_close(sock);

    } else if (strcmp(role, "pull-bind") == 0) {
        if (argc < 5) goto usage;
        double duration = atof(argv[4]);

        void *sock = zmq_socket(ctx, ZMQ_PULL);
        if (!sock) die("zmq_socket PULL");
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");
        report_bound_port(ctx, sock);
        wait_for_start_barrier();

        zmq_msg_t msg;
        zmq_msg_init(&msg);

        double warmup_end = now_secs() + 0.5;
        while (now_secs() < warmup_end) {
            int rc = zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT);
            if (rc < 0) {
                struct timespec ts = {0, 100000};
                nanosleep(&ts, NULL);
            }
        }

        long long count = 0;
        double cpu_before = cpu_time_secs();
        double t0 = now_secs();
        double deadline = t0 + duration;

        zmq_pollitem_t items[1];
        items[0].socket = sock;
        items[0].events = ZMQ_POLLIN;

        for (;;) {
            double remaining = deadline - now_secs();
            if (remaining <= 0) break;
            long timeout_ms = (long)(remaining * 1000.0);
            if (timeout_ms < 1) timeout_ms = 1;
            int rc = zmq_poll(items, 1, timeout_ms);
            if (rc < 0) break;
            if (rc == 0) break;
            if (items[0].revents & ZMQ_POLLIN) {
                while (zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT) >= 0) {
                    count++;
                    if (now_secs() >= deadline) goto done_pull_bind;
                }
            }
        }
done_pull_bind:;
        double elapsed = now_secs() - t0;
        double cpu = cpu_time_secs() - cpu_before;
        printf("%lld %.6f %d %.6f\n", count, elapsed, size, cpu);
        fflush(stdout);
        _exit(0);

    } else if (strcmp(role, "inproc") == 0) {
        if (argc < 5) goto usage;
        const char *name = argv[2];
        double duration = atof(argv[4]);

        InprocPushArg push_arg = { ctx, name, size };
        pthread_t tid;
        pthread_create(&tid, NULL, inproc_push_thread, &push_arg);

        char addr[256];
        snprintf(addr, sizeof(addr), "inproc://%s", name);

        void *sock = zmq_socket(ctx, ZMQ_PULL);
        if (!sock) die("zmq_socket PULL");
        if (zmq_connect(sock, addr) != 0) die("zmq_connect");

        zmq_msg_t msg;
        zmq_msg_init(&msg);

        double warmup_end = now_secs() + 0.5;
        while (now_secs() < warmup_end) {
            int rc = zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT);
            if (rc < 0) {
                struct timespec ts = {0, 100000};
                nanosleep(&ts, NULL);
            }
        }

        long long count = 0;
        double t0 = now_secs();
        double deadline = t0 + duration;

        zmq_pollitem_t items[1];
        items[0].socket = sock;
        items[0].events = ZMQ_POLLIN;

        for (;;) {
            double remaining = deadline - now_secs();
            if (remaining <= 0) break;
            long timeout_ms = (long)(remaining * 1000.0);
            if (timeout_ms < 1) timeout_ms = 1;
            int rc = zmq_poll(items, 1, timeout_ms);
            if (rc <= 0) break;
            if (items[0].revents & ZMQ_POLLIN) {
                while (zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT) >= 0) {
                    count++;
                    if (now_secs() >= deadline) goto done_inproc;
                }
            }
        }
done_inproc:;
        double elapsed = now_secs() - t0;
        printf("%lld %.6f %d\n", count, elapsed, size);

        zmq_msg_close(&msg);
        zmq_close(sock);
        /* zmq_send is not a pthread cancellation point; exit instead of
           joining to avoid blocking on the push thread's send loop. */
        exit(0);

    } else if (strcmp(role, "pub") == 0) {
        void *sock = zmq_socket(ctx, ZMQ_PUB);
        if (!sock) die("zmq_socket PUB");
        int block = 1;
        zmq_setsockopt(sock, ZMQ_XPUB_NODROP, &block, sizeof(block));
        if (bench_curve_enabled()) setup_curve_server(sock);
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");
        report_bound_port(ctx, sock);
        wait_for_start_barrier();

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        for (;;) {
            if (zmq_send(sock, buf, size, 0) < 0) break;
        }
        free(buf);
        zmq_close(sock);

    } else if (strcmp(role, "sub") == 0) {
        if (argc < 5) goto usage;
        double duration = atof(argv[4]);

        void *sock = zmq_socket(ctx, ZMQ_SUB);
        if (!sock) die("zmq_socket SUB");
        if (bench_curve_enabled()) setup_curve_client(sock);
        zmq_setsockopt(sock, ZMQ_SUBSCRIBE, "", 0);
        if (zmq_connect(sock, addr) != 0) die("zmq_connect");
        wait_for_start_barrier();

        zmq_msg_t msg;
        zmq_msg_init(&msg);

        double warmup_end = now_secs() + 0.5;
        while (now_secs() < warmup_end) {
            int rc = zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT);
            if (rc < 0) {
                struct timespec ts = {0, 100000};
                nanosleep(&ts, NULL);
            }
        }

        long long count = 0;
        double cpu_before = cpu_time_secs();
        double t0 = now_secs();
        double deadline = t0 + duration;

        zmq_pollitem_t items[1];
        items[0].socket = sock;
        items[0].events = ZMQ_POLLIN;

        for (;;) {
            double remaining = deadline - now_secs();
            if (remaining <= 0) break;
            long timeout_ms = (long)(remaining * 1000.0);
            if (timeout_ms < 1) timeout_ms = 1;
            int rc = zmq_poll(items, 1, timeout_ms);
            if (rc < 0) break;
            if (rc == 0) break;
            if (items[0].revents & ZMQ_POLLIN) {
                while (zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT) >= 0) {
                    count++;
                    if (now_secs() >= deadline) goto done_sub;
                }
            }
        }
done_sub:;
        double elapsed = now_secs() - t0;
        double cpu = cpu_time_secs() - cpu_before;
        printf("%lld %.6f %d %.6f\n", count, elapsed, size, cpu);

        zmq_msg_close(&msg);
        zmq_close(sock);

    } else if (strcmp(role, "inproc-pubsub") == 0) {
        if (argc < 5) goto usage;
        const char *name = argv[2];
        double duration = atof(argv[4]);
        int peers = argc >= 6 ? atoi(argv[5]) : 1;

        char inproc_addr[256];
        snprintf(inproc_addr, sizeof(inproc_addr), "inproc://%s", name);

        void *pub_sock = zmq_socket(ctx, ZMQ_PUB);
        if (!pub_sock) die("zmq_socket PUB");
        int block = 1;
        zmq_setsockopt(pub_sock, ZMQ_XPUB_NODROP, &block, sizeof(block));
        if (zmq_bind(pub_sock, inproc_addr) != 0) die("zmq_bind PUB");

        void *subs[64];
        int actual_peers = peers < 64 ? peers : 64;
        for (int i = 0; i < actual_peers; i++) {
            subs[i] = zmq_socket(ctx, ZMQ_SUB);
            if (!subs[i]) die("zmq_socket SUB");
            zmq_setsockopt(subs[i], ZMQ_SUBSCRIBE, "", 0);
            if (zmq_connect(subs[i], inproc_addr) != 0) die("zmq_connect");
        }

        InprocPubSendArg send_arg = { pub_sock, size };

        pthread_t pub_tid;
        pthread_create(&pub_tid, NULL, inproc_pub_send_thread, &send_arg);

        zmq_msg_t msg;
        zmq_msg_init(&msg);

        double warmup_deadline = now_secs() + 5.0;
        int got_first = 0;
        while (!got_first && now_secs() < warmup_deadline) {
            int rc = zmq_msg_recv(&msg, subs[0], ZMQ_DONTWAIT);
            if (rc >= 0) {
                got_first = 1;
            } else {
                struct timespec ts = {0, 1000000};
                nanosleep(&ts, NULL);
            }
        }
        double warmup_end = now_secs() + 0.5;
        while (now_secs() < warmup_end) {
            zmq_msg_recv(&msg, subs[0], ZMQ_DONTWAIT);
        }

        long long count = 0;
        double t0 = now_secs();
        double deadline = t0 + duration;

        zmq_pollitem_t items[1];
        items[0].socket = subs[0];
        items[0].events = ZMQ_POLLIN;

        for (;;) {
            double remaining = deadline - now_secs();
            if (remaining <= 0) break;
            long timeout_ms = (long)(remaining * 1000.0);
            if (timeout_ms < 1) timeout_ms = 1;
            int rc = zmq_poll(items, 1, timeout_ms);
            if (rc <= 0) break;
            if (items[0].revents & ZMQ_POLLIN) {
                while (zmq_msg_recv(&msg, subs[0], ZMQ_DONTWAIT) >= 0) {
                    count++;
                    if (now_secs() >= deadline) goto done_inproc_pubsub;
                }
            }
        }
done_inproc_pubsub:;
        double elapsed = now_secs() - t0;
        printf("%lld %.6f %d\n", count, elapsed, size);

        zmq_msg_close(&msg);
        for (int i = 0; i < actual_peers; i++) zmq_close(subs[i]);
        exit(0);

    } else if (strcmp(role, "multi-sub") == 0) {
        if (argc < 6) goto usage;
        int count = atoi(argv[5]);
        if (count < 1 || count > 256) {
            fprintf(stderr, "socket_count must be 1..256\n");
            return 1;
        }
        run_multi_sub(ctx, addr, size, atof(argv[4]), count);

    } else if (strcmp(role, "multi-pull") == 0) {
        if (argc < 6) goto usage;
        double duration = atof(argv[4]);
        int count = atoi(argv[5]);
        if (count < 1 || count > 256) {
            fprintf(stderr, "socket_count must be 1..256\n");
            return 1;
        }

        RecvWorker workers[256];
        void *sockets[256];
        pthread_t threads[256];

        for (int i = 0; i < count; i++) {
            sockets[i] = zmq_socket(ctx, ZMQ_PULL);
            if (!sockets[i]) die("zmq_socket PULL");
            if (zmq_connect(sockets[i], addr) != 0) die("zmq_connect");
            workers[i].sock = sockets[i];
            atomic_store(&workers[i].counter, 0);
            workers[i].stop = 0;
        }

        wait_for_start_barrier();

        /* warmup: let sockets receive for 500 ms, then zero counters */
        for (int i = 0; i < count; i++)
            pthread_create(&threads[i], NULL, recv_thread, &workers[i]);

        struct timespec warmup_ts = {0, 500000000};
        nanosleep(&warmup_ts, NULL);
        for (int i = 0; i < count; i++)
            atomic_store(&workers[i].counter, 0);

        double cpu_before = cpu_time_secs();
        double t0 = now_secs();
        double deadline_secs = duration;
        long sleep_secs = (long)deadline_secs;
        long sleep_ns = (long)((deadline_secs - sleep_secs) * 1e9);
        struct timespec dur_ts = { sleep_secs, sleep_ns };
        nanosleep(&dur_ts, NULL);
        double elapsed = now_secs() - t0;
        double cpu = cpu_time_secs() - cpu_before;

        for (int i = 0; i < count; i++)
            workers[i].stop = 1;

        /* unblock threads stuck in zmq_msg_recv by setting a short timeout */
        int timeout_ms = 1;
        for (int i = 0; i < count; i++)
            zmq_setsockopt(sockets[i], ZMQ_RCVTIMEO, &timeout_ms,
                           sizeof(timeout_ms));
        for (int i = 0; i < count; i++)
            pthread_join(threads[i], NULL);

        long long total = 0;
        double rates[256];
        double per_min = 1e18, per_max = 0;
        for (int i = 0; i < count; i++) {
            long long c = atomic_load(&workers[i].counter);
            total += c;
            rates[i] = (double)c / elapsed;
            if (rates[i] < per_min) per_min = rates[i];
            if (rates[i] > per_max) per_max = rates[i];
        }
        printf("%lld %.6f %d %.6f %d %.1f %.1f",
               total, elapsed, size, cpu, count, per_min, per_max);
        print_fairness(rates, count, elapsed);

        for (int i = 0; i < count; i++)
            zmq_close(sockets[i]);

    } else if (strcmp(role, "multi-push") == 0) {
        if (argc < 5) goto usage;
        int count = atoi(argv[4]);
        if (count < 1 || count > 256) {
            fprintf(stderr, "socket_count must be 1..256\n");
            return 1;
        }
        double duration = (argc >= 6) ? atof(argv[5]) : 0;

        SendWorker workers[256];
        void *sockets[256];
        pthread_t threads[256];

        for (int i = 0; i < count; i++) {
            sockets[i] = zmq_socket(ctx, ZMQ_PUSH);
            if (!sockets[i]) die("zmq_socket PUSH");
            if (zmq_connect(sockets[i], addr) != 0) die("zmq_connect");
            workers[i].sock = sockets[i];
            workers[i].size = size;
            atomic_store(&workers[i].counter, 0);
            workers[i].stop = 0;
        }

        wait_for_start_barrier();

        for (int i = 0; i < count; i++)
            pthread_create(&threads[i], NULL, send_thread, &workers[i]);

        if (duration > 0) {
            long sleep_secs = (long)duration;
            long sleep_ns = (long)((duration - sleep_secs) * 1e9);
            struct timespec dur_ts = { sleep_secs, sleep_ns };

            double cpu_before = cpu_time_secs();
            double t0 = now_secs();
            nanosleep(&dur_ts, NULL);
            double elapsed = now_secs() - t0;
            double cpu = cpu_time_secs() - cpu_before;

            for (int i = 0; i < count; i++)
                workers[i].stop = 1;

            int timeout_ms = 1;
            for (int i = 0; i < count; i++)
                zmq_setsockopt(sockets[i], ZMQ_SNDTIMEO, &timeout_ms,
                               sizeof(timeout_ms));
            for (int i = 0; i < count; i++)
                pthread_join(threads[i], NULL);

            long long total = 0;
            double rates[256];
            double per_min = 1e18, per_max = 0;
            for (int i = 0; i < count; i++) {
                long long c = atomic_load(&workers[i].counter);
                total += c;
                rates[i] = (double)c / elapsed;
                if (rates[i] < per_min) per_min = rates[i];
                if (rates[i] > per_max) per_max = rates[i];
            }
            printf("%lld %.6f %d %.6f %d %.1f %.1f",
                   total, elapsed, size, cpu, count, per_min, per_max);
            print_fairness(rates, count, elapsed);
        } else {
            for (int i = 0; i < count; i++)
                pthread_join(threads[i], NULL);
        }

        for (int i = 0; i < count; i++)
            zmq_close(sockets[i]);

    } else if (strcmp(role, "rep") == 0) {
        void *sock = latency_socket(ctx, 0);
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");
        report_bound_port(ctx, sock);

        latency_echo(sock, size);
        zmq_close(sock);

    } else if (strcmp(role, "req") == 0) {
        if (argc < 6) goto usage;
        int iterations = atoi(argv[4]);
        int warmup = atoi(argv[5]);

        int routed = latency_socket_type(1) == ZMQ_ROUTER;
        void *sock = latency_socket(ctx, 1);
        if (zmq_connect(sock, addr) != 0) die("zmq_connect");

        struct timespec sleep_ts = {0, 200000000};
        nanosleep(&sleep_ts, NULL);

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        zmq_msg_t identity, reply;
        zmq_msg_init(&identity);
        zmq_msg_init(&reply);

        latency_exchange(sock, buf, size, routed, &identity, &reply);
        validate_latency_message(&reply, &identity, routed, 1, size);
        for (int i = 0; i < warmup; i++) {
            latency_exchange(sock, buf, size, routed, &identity, &reply);
        }

        uint64_t *rtts = malloc(sizeof(uint64_t) * iterations);
        if (!rtts) { perror("malloc"); exit(1); }

        double wall_t0 = now_secs();
        double cpu_before = cpu_time_secs();
        for (int i = 0; i < iterations; i++) {
            struct timespec t0, t1;
            clock_gettime(CLOCK_MONOTONIC, &t0);
            latency_exchange(sock, buf, size, routed, &identity, &reply);
            clock_gettime(CLOCK_MONOTONIC, &t1);
            rtts[i] = (uint64_t)(t1.tv_sec - t0.tv_sec) * 1000000000ULL
                     + (uint64_t)(t1.tv_nsec - t0.tv_nsec);
        }
        double cpu = cpu_time_secs() - cpu_before;
        double wall_elapsed = now_secs() - wall_t0;

        qsort(rtts, iterations, sizeof(uint64_t), cmp_u64);

        double p50  = percentile_u64(rtts, iterations, 50);
        double p99  = percentile_u64(rtts, iterations, 99);
        double p999 = percentile_u64(rtts, iterations, 99.9);
        double max  = rtts[iterations - 1] / 1000.0;
        printf("%.3f %.3f %.3f %.3f %d %.6f %.6f\n", p50, p99, p999, max, iterations, cpu, wall_elapsed);

        free(rtts);
        free(buf);
        zmq_msg_close(&identity);
        zmq_msg_close(&reply);
        zmq_close(sock);

    } else if (strcmp(role, "inproc-latency") == 0) {
        if (argc < 6) goto usage;
        const char *name = argv[2];
        int iterations = atoi(argv[4]);
        int warmup = atoi(argv[5]);

        /* REP thread */
        InprocRepArg rep_arg = { ctx, name, size };

        pthread_t tid;
        pthread_create(&tid, NULL, inproc_rep_thread, &rep_arg);

        char inproc_addr[256];
        snprintf(inproc_addr, sizeof(inproc_addr), "inproc://%s", name);

        struct timespec sleep_ts = {0, 200000000};
        nanosleep(&sleep_ts, NULL);

        int routed = latency_socket_type(1) == ZMQ_ROUTER;
        void *sock = latency_socket(ctx, 1);
        if (zmq_connect(sock, inproc_addr) != 0) die("zmq_connect");

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        zmq_msg_t identity, reply;
        zmq_msg_init(&identity);
        zmq_msg_init(&reply);

        latency_exchange(sock, buf, size, routed, &identity, &reply);
        validate_latency_message(&reply, &identity, routed, 1, size);
        for (int i = 0; i < warmup; i++) {
            latency_exchange(sock, buf, size, routed, &identity, &reply);
        }

        uint64_t *rtts = malloc(sizeof(uint64_t) * iterations);
        if (!rtts) { perror("malloc"); exit(1); }

        struct timespec wall_start, wall_end;
        clock_gettime(CLOCK_MONOTONIC, &wall_start);
        for (int i = 0; i < iterations; i++) {
            struct timespec t0, t1;
            clock_gettime(CLOCK_MONOTONIC, &t0);
            latency_exchange(sock, buf, size, routed, &identity, &reply);
            clock_gettime(CLOCK_MONOTONIC, &t1);
            rtts[i] = (uint64_t)(t1.tv_sec - t0.tv_sec) * 1000000000ULL
                     + (uint64_t)(t1.tv_nsec - t0.tv_nsec);
        }
        clock_gettime(CLOCK_MONOTONIC, &wall_end);

        qsort(rtts, iterations, sizeof(uint64_t), cmp_u64);

        double p50  = percentile_u64(rtts, iterations, 50);
        double p99  = percentile_u64(rtts, iterations, 99);
        double p999 = percentile_u64(rtts, iterations, 99.9);
        double max  = rtts[iterations - 1] / 1000.0;
        double wall_elapsed = (double)(wall_end.tv_sec - wall_start.tv_sec)
                             + (double)(wall_end.tv_nsec - wall_start.tv_nsec) / 1e9;
        printf("%.3f %.3f %.3f %.3f %d 0 %.6f\n", p50, p99, p999, max, iterations, wall_elapsed);

        free(rtts);
        free(buf);
        zmq_msg_close(&identity);
        zmq_msg_close(&reply);
        zmq_close(sock);
        exit(0);

    } else {
        goto usage;
    }

    zmq_ctx_destroy(ctx);
    return 0;

usage:
    fprintf(stderr, "usage: %s push <addr> <size>\n", argv[0]);
    fprintf(stderr, "       %s pull <addr> <size> <duration_secs>\n", argv[0]);
    fprintf(stderr, "       %s multi-pull <addr> <size> <duration_secs> <count>\n", argv[0]);
    fprintf(stderr, "       %s multi-sub <addr> <size> <duration_secs> <count>\n", argv[0]);
    fprintf(stderr, "       %s multi-push <addr> <size> <count> [duration_secs]\n", argv[0]);
    fprintf(stderr, "       %s inproc <name> <size> <duration_secs>\n", argv[0]);
    fprintf(stderr, "       %s rep <addr> <size>\n", argv[0]);
    fprintf(stderr, "       %s req <addr> <size> <iterations> <warmup>\n", argv[0]);
    fprintf(stderr, "       %s inproc-latency <name> <size> <iterations> <warmup>\n", argv[0]);
    fprintf(stderr, "<addr>: port number or full ZMQ address (tcp:// ipc://)\n");
    return 1;
}
