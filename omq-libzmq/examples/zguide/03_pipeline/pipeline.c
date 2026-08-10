#include "zg.h"

static void ventilator(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-03-ventilator-c");
    int n_tasks = zg_arg_int(argc, argv, 3, 1000);
    void *ctx = zg_ctx();
    void *push = zg_socket(ctx, ZMQ_PUSH);
    int linger = 2000;
    zmq_setsockopt(push, ZMQ_LINGER, &linger, sizeof(linger));

    zg_bind(push, ep);
    zg_sleep_ms(500);

    for (int i = 0; i < n_tasks; i++) {
        char msg[32];
        snprintf(msg, sizeof(msg), "task-%d", i);
        zg_send_str(push, msg);
    }

    printf("ventilator: sent %d tasks on %s\n", n_tasks, ep);
    zg_close(push);
    zg_term(ctx);
}

static void worker(int argc, char **argv) {
    const char *vent_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-03-ventilator-c");
    const char *sink_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-03-sink-c");
    const char *id = zg_arg(argc, argv, 4, "0");
    void *ctx = zg_ctx();
    void *pull = zg_socket(ctx, ZMQ_PULL);
    void *push = zg_socket(ctx, ZMQ_PUSH);

    zg_connect(pull, vent_ep);
    zg_connect(push, sink_ep);
    printf("worker-%s: ready\n", id);

    for (;;) {
        char *body = zg_recv_str(pull);
        char *result = zg_printf_alloc("worker-%s:%s", id, body);
        zg_send_str(push, result);
        free(body);
        free(result);
    }
}

static void sink(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-03-sink-c");
    int expected = zg_arg_int(argc, argv, 3, 1000);
    void *ctx = zg_ctx();
    void *pull = zg_socket(ctx, ZMQ_PULL);
    int counts[16] = {0};

    zg_bind(pull, ep);
    printf("sink: listening on %s, expecting %d results\n", ep, expected);

    for (int i = 0; i < expected; i++) {
        char *body = zg_recv_str(pull);
        int worker = -1;
        sscanf(body, "worker-%d:", &worker);
        if (worker >= 0 && worker < 16) {
            counts[worker]++;
        }
        if ((i + 1) % 25 == 0 || i + 1 == expected) {
            printf("sink: received %d/%d\n", i + 1, expected);
        }
        free(body);
    }

    printf("sink: done -- %d results\n", expected);
    for (int i = 0; i < 16; i++) {
        if (counts[i] > 0) {
            printf("  worker-%d: %d items\n", i, counts[i]);
        }
    }
    zg_close(pull);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "ventilator");
    if (strcmp(role, "ventilator") == 0) {
        ventilator(argc, argv);
    } else if (strcmp(role, "worker") == 0) {
        worker(argc, argv);
    } else if (strcmp(role, "sink") == 0) {
        sink(argc, argv);
    } else {
        fprintf(stderr, "usage: %s ventilator|worker|sink [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
