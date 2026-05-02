/*
 * Two-process TCP throughput peer for libzmq 4.x.
 *
 * Usage:
 *   libzmq_bench_peer push <port> <msg_size_bytes>
 *   libzmq_bench_peer pull <port> <msg_size_bytes> <duration_secs>
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

static double now_secs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void die(const char *msg) {
    fprintf(stderr, "%s: %s\n", msg, zmq_strerror(zmq_errno()));
    exit(1);
}

int main(int argc, char **argv) {
    if (argc < 4) goto usage;

    const char *role = argv[1];
    int port        = atoi(argv[2]);
    int size        = atoi(argv[3]);

    void *ctx = zmq_ctx_new();
    if (!ctx) die("zmq_ctx_new");

    char addr[64];
    snprintf(addr, sizeof(addr), "tcp://127.0.0.1:%d", port);

    if (strcmp(role, "push") == 0) {
        void *sock = zmq_socket(ctx, ZMQ_PUSH);
        if (!sock) die("zmq_socket PUSH");
        if (zmq_bind(sock, addr) != 0) die("zmq_bind");

        char *buf = calloc(1, size);
        if (!buf) { perror("calloc"); exit(1); }
        memset(buf, 'x', size);

        for (;;) {
            if (zmq_send(sock, buf, size, 0) < 0) {
                /* EINTR on SIGTERM is fine; anything else is a bug */
                if (zmq_errno() == EINTR) break;
                die("zmq_send");
            }
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

        /* Warmup: drain for 500 ms. */
        double warmup_end = now_secs() + 0.5;
        while (now_secs() < warmup_end) {
            int rc = zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT);
            if (rc < 0) {
                /* Nothing ready; busy-spin is fine for 500 ms warmup. */
                struct timespec ts = {0, 100000}; /* 100 µs */
                nanosleep(&ts, NULL);
            }
        }

        /* Timed window. Use blocking recv with a poll timeout. */
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
            if (rc == 0) break; /* timeout */
            if (items[0].revents & ZMQ_POLLIN) {
                while (zmq_msg_recv(&msg, sock, ZMQ_DONTWAIT) >= 0) {
                    count++;
                    if (now_secs() >= deadline) goto done;
                }
            }
        }
done:;
        double elapsed = now_secs() - t0;
        printf("%lld %.6f %d\n", count, elapsed, size);

        zmq_msg_close(&msg);
        zmq_close(sock);

    } else {
        goto usage;
    }

    zmq_ctx_destroy(ctx);
    return 0;

usage:
    fprintf(stderr, "usage: %s push <port> <size>\n", argv[0]);
    fprintf(stderr, "       %s pull <port> <size> <duration_secs>\n", argv[0]);
    return 1;
}
