#include "zg.h"

static void echo(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-01-echo-c");
    void *ctx = zg_ctx();
    void *rep = zg_socket(ctx, ZMQ_REP);
    void *req = zg_socket(ctx, ZMQ_REQ);

    zg_bind(rep, ep);
    zg_connect(req, ep);
    zg_sleep_ms(50);

    for (int i = 0; i < 3; i++) {
        char request[32];
        snprintf(request, sizeof(request), "hello-%d", i);
        zg_send_str(req, request);

        char *body = zg_recv_str(rep);
        char *reply = zg_printf_alloc("echo:%s", body);
        zg_send_str(rep, reply);
        free(body);

        char *got = zg_recv_str(req);
        printf("client: %s -> %s\n", request, got);
        free(got);
        free(reply);
    }

    printf("done: 3 request-reply cycles\n");
    zg_close(req);
    zg_close(rep);
    zg_term(ctx);
}

static void broker(int argc, char **argv) {
    const char *frontend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-01-frontend-c");
    const char *backend_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-01-backend-c");
    void *ctx = zg_ctx();
    void *frontend = zg_socket(ctx, ZMQ_ROUTER);
    void *backend = zg_socket(ctx, ZMQ_DEALER);

    zg_bind(frontend, frontend_ep);
    zg_bind(backend, backend_ep);
    printf("broker: frontend=%s backend=%s\n", frontend_ep, backend_ep);

    zg_check(zmq_proxy(frontend, backend, NULL), "zmq_proxy");
}

static void worker(int argc, char **argv) {
    const char *backend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-01-backend-c");
    const char *id = zg_arg(argc, argv, 3, "0");
    void *ctx = zg_ctx();
    void *rep = zg_socket(ctx, ZMQ_REP);

    zg_connect(rep, backend_ep);
    printf("worker-%s: ready\n", id);

    for (;;) {
        char *body = zg_recv_str(rep);
        if (body == NULL) {
            zg_die("zmq_recv");
        }
        char *reply = zg_printf_alloc("worker-%s:%s", id, body);
        printf("worker-%s: %s -> %s\n", id, body, reply);
        zg_send_str(rep, reply);
        free(body);
        free(reply);
    }
}

static void client(int argc, char **argv) {
    const char *frontend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-01-frontend-c");
    int n = zg_arg_int(argc, argv, 3, 9);
    void *ctx = zg_ctx();
    void *req = zg_socket(ctx, ZMQ_REQ);

    zg_connect(req, frontend_ep);
    zg_sleep_ms(100);

    for (int i = 0; i < n; i++) {
        char request[32];
        snprintf(request, sizeof(request), "request-%d", i);
        zg_send_str(req, request);
        char *reply = zg_recv_str(req);
        printf("client: %s -> %s\n", request, reply);
        free(reply);
    }

    printf("done: %d replies\n", n);
    zg_close(req);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "echo");
    if (strcmp(role, "echo") == 0) {
        echo(argc, argv);
    } else if (strcmp(role, "broker") == 0) {
        broker(argc, argv);
    } else if (strcmp(role, "worker") == 0) {
        worker(argc, argv);
    } else if (strcmp(role, "client") == 0) {
        client(argc, argv);
    } else {
        fprintf(stderr, "usage: %s echo|broker|worker|client [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
