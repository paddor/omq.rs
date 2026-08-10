#include "zg.h"

static void server(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-04-server-c");
    void *ctx = zg_ctx();
    void *rep = zg_socket(ctx, ZMQ_REP);
    int handled = 0;

    zg_bind(rep, ep);
    zg_set_i32(rep, ZMQ_RCVTIMEO, 3000);

    for (;;) {
        char *body = zg_recv_str(rep);
        if (body == NULL) {
            if (zmq_errno() == EAGAIN) {
                printf("server: no request for 3s, exiting\n");
                break;
            }
            zg_die("zmq_recv");
        }
        handled++;
        if (handled == 3) {
            printf("server: simulating crash on '%s'\n", body);
            zg_sleep_ms(500);
        }
        char *reply = zg_printf_alloc("reply:%s", body);
        if (zmq_send(rep, reply, strlen(reply), 0) >= 0) {
            printf("server: replied to %s\n", body);
        } else {
            printf("server: dropped stale reply for %s\n", body);
        }
        free(body);
        free(reply);
    }

    printf("server: handled %d requests\n", handled);
    zg_close(rep);
    zg_term(ctx);
}

static void *new_req(void *ctx, const char *ep) {
    void *req = zg_socket(ctx, ZMQ_REQ);
    zg_connect(req, ep);
    zg_sleep_ms(20);
    return req;
}

static void client(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-04-server-c");
    void *ctx = zg_ctx();
    void *req = new_req(ctx, ep);
    int total_retries = 0;
    int replies = 0;

    for (int seq = 0; seq < 5; seq++) {
        char request[32];
        snprintf(request, sizeof(request), "request-%d", seq);
        int attempts = 0;

        for (;;) {
            zg_send_str(req, request);
            char *reply = zg_recv_str_timeout(req, 400);
            if (reply != NULL) {
                printf("client: %s -> %s\n", request, reply);
                free(reply);
                replies++;
                break;
            }

            attempts++;
            total_retries++;
            printf("client: timeout on %s, retry %d\n", request, attempts);
            zg_close(req);
            req = new_req(ctx, ep);
            if (attempts >= 3) {
                printf("client: giving up on %s\n", request);
                break;
            }
        }
    }

    printf("done: %d replies, %d retries\n", replies, total_retries);
    zg_close(req);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "client");
    if (strcmp(role, "server") == 0) {
        server(argc, argv);
    } else if (strcmp(role, "client") == 0) {
        client(argc, argv);
    } else {
        fprintf(stderr, "usage: %s server|client [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
