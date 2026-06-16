/*
 * Two-process throughput peer for libzmq 4.x.
 *
 * Usage:
 *   libzmq_bench_peer push <addr> <msg_size_bytes>
 *   libzmq_bench_peer pull <addr> <msg_size_bytes> <duration_secs>
 *   libzmq_bench_peer rep  <addr> <msg_size_bytes>
 *   libzmq_bench_peer req  <addr> <msg_size_bytes> <iterations> <warmup>
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
 * Compile: gcc -O2 -o libzmq_bench_peer libzmq_bench_peer.c -lzmq
 *
 * Output (pull only, one line to stdout):
 *   <count> <elapsed_secs> <msg_size>
 */

#include <zmq.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/resource.h>

static double now_secs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
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

static void print_bound_port(void *sock) {
    char ep[256];
    size_t ep_len = sizeof(ep);
    if (zmq_getsockopt(sock, ZMQ_LAST_ENDPOINT, ep, &ep_len) == 0) {
        char *colon = strrchr(ep, ':');
        if (colon) {
            printf("PORT %s\n", colon + 1);
            fflush(stdout);
        }
    }
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

typedef struct { void *ctx; const char *name; int size; } InprocPushArg;

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

int main(int argc, char **argv) {
    if (argc < 4) goto usage;

    const char *role = argv[1];
    char addr_buf[256];
    const char *addr = resolve_addr(argv[2], addr_buf, sizeof(addr_buf));
    int size = atoi(argv[3]);

    void *ctx = zmq_ctx_new();
    if (!ctx) die("zmq_ctx_new");

    if (strcmp(role, "push") == 0) {
        void *sock = zmq_socket(ctx, ZMQ_PUSH);
        if (!sock) die("zmq_socket PUSH");
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");
        print_bound_port(sock);

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
        print_bound_port(sock);

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
        printf("%lld %.6f %d\n", count, elapsed, size);

        zmq_msg_close(&msg);
        zmq_close(sock);

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
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");
        print_bound_port(sock);

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
        zmq_setsockopt(sock, ZMQ_SUBSCRIBE, "", 0);
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
        printf("%lld %.6f %d\n", count, elapsed, size);

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

        typedef struct { void *sock; int size; } InprocPubSendArg;
        InprocPubSendArg send_arg = { pub_sock, size };

        void *inproc_pub_send_thread(void *arg_) {
            InprocPubSendArg *a = arg_;
            char *buf = calloc(1, a->size);
            memset(buf, 'x', a->size);
            for (;;) {
                if (zmq_send(a->sock, buf, a->size, 0) < 0) break;
            }
            free(buf);
            return NULL;
        }

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

    } else if (strcmp(role, "rep") == 0) {
        void *sock = zmq_socket(ctx, ZMQ_REP);
        if (!sock) die("zmq_socket REP");
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");
        print_bound_port(sock);

        zmq_msg_t msg;
        zmq_msg_init(&msg);
        for (;;) {
            int rc = zmq_msg_recv(&msg, sock, 0);
            if (rc < 0) break;
            int sz = zmq_msg_size(&msg);
            if (zmq_send(sock, zmq_msg_data(&msg), sz, 0) < 0) break;
        }
        zmq_msg_close(&msg);
        zmq_close(sock);

    } else if (strcmp(role, "req") == 0) {
        if (argc < 6) goto usage;
        int iterations = atoi(argv[4]);
        int warmup = atoi(argv[5]);

        void *sock = zmq_socket(ctx, ZMQ_REQ);
        if (!sock) die("zmq_socket REQ");
        if (zmq_connect(sock, addr) != 0) die("zmq_connect");

        struct timespec sleep_ts = {0, 200000000};
        nanosleep(&sleep_ts, NULL);

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        zmq_msg_t reply;
        zmq_msg_init(&reply);

        for (int i = 0; i < warmup; i++) {
            if (zmq_send(sock, buf, size, 0) < 0) die("zmq_send warmup");
            if (zmq_msg_recv(&reply, sock, 0) < 0) die("zmq_recv warmup");
        }

        uint64_t *rtts = malloc(sizeof(uint64_t) * iterations);
        if (!rtts) { perror("malloc"); exit(1); }

        double wall_t0 = now_secs();
        double cpu_before = cpu_time_secs();
        for (int i = 0; i < iterations; i++) {
            struct timespec t0, t1;
            clock_gettime(CLOCK_MONOTONIC, &t0);
            if (zmq_send(sock, buf, size, 0) < 0) break;
            if (zmq_msg_recv(&reply, sock, 0) < 0) die("zmq_recv");
            clock_gettime(CLOCK_MONOTONIC, &t1);
            rtts[i] = (uint64_t)(t1.tv_sec - t0.tv_sec) * 1000000000ULL
                     + (uint64_t)(t1.tv_nsec - t0.tv_nsec);
        }
        double cpu = cpu_time_secs() - cpu_before;
        double wall_elapsed = now_secs() - wall_t0;

        int cmp_u64(const void *a, const void *b) {
            uint64_t va = *(const uint64_t *)a;
            uint64_t vb = *(const uint64_t *)b;
            return (va > vb) - (va < vb);
        }
        qsort(rtts, iterations, sizeof(uint64_t), cmp_u64);

        double percentile(uint64_t *sorted, int n, double p) {
            int idx = (int)(n * p / 100.0);
            if (idx >= n) idx = n - 1;
            return sorted[idx] / 1000.0;
        }

        double p50  = percentile(rtts, iterations, 50);
        double p99  = percentile(rtts, iterations, 99);
        double p999 = percentile(rtts, iterations, 99.9);
        double max  = rtts[iterations - 1] / 1000.0;
        printf("%.3f %.3f %.3f %.3f %d %.6f %.6f\n", p50, p99, p999, max, iterations, cpu, wall_elapsed);

        free(rtts);
        free(buf);
        zmq_msg_close(&reply);
        zmq_close(sock);

    } else if (strcmp(role, "inproc-latency") == 0) {
        if (argc < 6) goto usage;
        const char *name = argv[2];
        int iterations = atoi(argv[4]);
        int warmup = atoi(argv[5]);

        /* REP thread */
        typedef struct { void *ctx; const char *name; int size; } InprocRepArg;
        InprocRepArg rep_arg = { ctx, name, size };

        void *inproc_rep_thread(void *arg_) {
            InprocRepArg *a = arg_;
            char addr[256];
            snprintf(addr, sizeof(addr), "inproc://%s", a->name);
            void *sock = zmq_socket(a->ctx, ZMQ_REP);
            if (!sock || zmq_bind(sock, addr) != 0) return NULL;
            zmq_msg_t msg;
            zmq_msg_init(&msg);
            for (;;) {
                int rc = zmq_msg_recv(&msg, sock, 0);
                if (rc < 0) break;
                int sz = zmq_msg_size(&msg);
                if (zmq_send(sock, zmq_msg_data(&msg), sz, 0) < 0) break;
            }
            zmq_msg_close(&msg);
            zmq_close(sock);
            return NULL;
        }

        pthread_t tid;
        pthread_create(&tid, NULL, inproc_rep_thread, &rep_arg);

        char inproc_addr[256];
        snprintf(inproc_addr, sizeof(inproc_addr), "inproc://%s", name);

        struct timespec sleep_ts = {0, 200000000};
        nanosleep(&sleep_ts, NULL);

        void *sock = zmq_socket(ctx, ZMQ_REQ);
        if (!sock) die("zmq_socket REQ");
        if (zmq_connect(sock, inproc_addr) != 0) die("zmq_connect");

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        zmq_msg_t reply;
        zmq_msg_init(&reply);

        for (int i = 0; i < warmup; i++) {
            if (zmq_send(sock, buf, size, 0) < 0) die("zmq_send warmup");
            if (zmq_msg_recv(&reply, sock, 0) < 0) die("zmq_recv warmup");
        }

        uint64_t *rtts = malloc(sizeof(uint64_t) * iterations);
        if (!rtts) { perror("malloc"); exit(1); }

        struct timespec wall_start, wall_end;
        clock_gettime(CLOCK_MONOTONIC, &wall_start);
        for (int i = 0; i < iterations; i++) {
            struct timespec t0, t1;
            clock_gettime(CLOCK_MONOTONIC, &t0);
            if (zmq_send(sock, buf, size, 0) < 0) break;
            if (zmq_msg_recv(&reply, sock, 0) < 0) die("zmq_recv");
            clock_gettime(CLOCK_MONOTONIC, &t1);
            rtts[i] = (uint64_t)(t1.tv_sec - t0.tv_sec) * 1000000000ULL
                     + (uint64_t)(t1.tv_nsec - t0.tv_nsec);
        }
        clock_gettime(CLOCK_MONOTONIC, &wall_end);

        int cmp_u64(const void *a, const void *b) {
            uint64_t va = *(const uint64_t *)a;
            uint64_t vb = *(const uint64_t *)b;
            return (va > vb) - (va < vb);
        }
        qsort(rtts, iterations, sizeof(uint64_t), cmp_u64);

        double percentile(uint64_t *sorted, int n, double p) {
            int idx = (int)(n * p / 100.0);
            if (idx >= n) idx = n - 1;
            return sorted[idx] / 1000.0;
        }

        double p50  = percentile(rtts, iterations, 50);
        double p99  = percentile(rtts, iterations, 99);
        double p999 = percentile(rtts, iterations, 99.9);
        double max  = rtts[iterations - 1] / 1000.0;
        double wall_elapsed = (double)(wall_end.tv_sec - wall_start.tv_sec)
                             + (double)(wall_end.tv_nsec - wall_start.tv_nsec) / 1e9;
        printf("%.3f %.3f %.3f %.3f %d 0 %.6f\n", p50, p99, p999, max, iterations, wall_elapsed);

        free(rtts);
        free(buf);
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
    fprintf(stderr, "       %s inproc <name> <size> <duration_secs>\n", argv[0]);
    fprintf(stderr, "       %s rep <addr> <size>\n", argv[0]);
    fprintf(stderr, "       %s req <addr> <size> <iterations> <warmup>\n", argv[0]);
    fprintf(stderr, "       %s inproc-latency <name> <size> <iterations> <warmup>\n", argv[0]);
    fprintf(stderr, "<addr>: port number or full ZMQ address (tcp:// ipc://)\n");
    return 1;
}
