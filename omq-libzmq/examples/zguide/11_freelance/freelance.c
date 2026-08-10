#include "zg.h"

static void server(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-11-server1-c");
    const char *name = zg_arg(argc, argv, 3, "server");
    double delay_secs = zg_arg_double(argc, argv, 4, 0.0);
    void *ctx = zg_ctx();
    void *rep = zg_socket(ctx, ZMQ_REP);

    zg_bind(rep, ep);
    for (;;) {
        char *body = zg_recv_str(rep);
        if (delay_secs > 0.0) {
            zg_sleep_ms((int)(delay_secs * 1000.0));
        }
        printf("%s: served %s\n", name, body);
        char *reply = zg_printf_alloc("%s:%s", name, body);
        zg_send_str(rep, reply);
        free(body);
        free(reply);
    }
}

static char *req_attempt(void *ctx, const char *ep, const char *body, int timeout_ms) {
    void *req = zg_socket(ctx, ZMQ_REQ);
    zg_connect(req, ep);
    zg_sleep_ms(20);
    zg_send_str(req, body);
    char *reply = zg_recv_str_timeout(req, timeout_ms);
    zg_close(req);
    return reply;
}

static void client_sequential(int argc, char **argv) {
    const char *defaults[] = {
        "ipc://@omq-zguide-11-server1-c",
        "ipc://@omq-zguide-11-server2-c",
        "ipc://@omq-zguide-11-server3-c",
    };
    int ep_count = argc > 2 ? argc - 2 : 3;
    void *ctx = zg_ctx();

    for (int i = 0; i < 3; i++) {
        char body[32];
        snprintf(body, sizeof(body), "request-%d", i);
        bool served = false;

        for (int j = 0; j < ep_count; j++) {
            const char *ep = argc > 2 ? argv[j + 2] : defaults[j];
            char *reply = req_attempt(ctx, ep, body, 150);
            if (reply != NULL) {
                printf("client: %s -> %s\n", body, reply);
                free(reply);
                served = true;
                break;
            }
            printf("client: timeout on %s, trying next\n", ep);
        }

        if (!served) {
            fprintf(stderr, "client: all endpoints failed for %s\n", body);
        }
    }

    printf("client: done (3 requests)\n");
    zg_term(ctx);
}

static void client_shotgun(int argc, char **argv) {
    const char *defaults[] = {
        "ipc://@omq-zguide-11-server1-c",
        "ipc://@omq-zguide-11-server2-c",
    };
    int ep_count = argc > 2 ? argc - 2 : 2;
    void *ctx = zg_ctx();
    void *dealer = zg_socket(ctx, ZMQ_DEALER);

    for (int i = 0; i < ep_count; i++) {
        const char *ep = argc > 2 ? argv[i + 2] : defaults[i];
        zg_connect(dealer, ep);
    }
    zg_sleep_ms(50);

    for (int i = 0; i < ep_count; i++) {
        zg_send_more(dealer, "");
        zg_send_str(dealer, "shotgun-req");
    }

    char *reply = zg_recv_str_timeout(dealer, 1000);
    if (reply != NULL) {
        printf("client: first reply = %s\n", reply);
        free(reply);
    } else {
        fprintf(stderr, "client: timeout waiting for reply\n");
    }
    printf("client: done\n");
    zg_close(dealer);
    zg_term(ctx);
}

static void client_tracked(int argc, char **argv) {
    const char *defaults[] = {
        "ipc://@omq-zguide-11-server1-c",
        "ipc://@omq-zguide-11-server2-c",
    };
    int ep_count = argc > 2 ? argc - 2 : 2;
    int known_good = -1;
    void *ctx = zg_ctx();

    for (int i = 0; i < 6; i++) {
        char body[32];
        snprintf(body, sizeof(body), "request-%d", i);
        bool served = false;

        for (int pass = 0; pass < ep_count; pass++) {
            int idx = known_good >= 0 ? (known_good + pass) % ep_count : pass;
            const char *ep = argc > 2 ? argv[idx + 2] : defaults[idx];
            char *reply = req_attempt(ctx, ep, body, 200);
            if (reply != NULL) {
                printf("client: %s -> %s (via %s)\n", body, reply, ep);
                free(reply);
                known_good = idx;
                served = true;
                break;
            }
            printf("client: %s timed out, rotating\n", ep);
            if (known_good == idx) {
                known_good = -1;
            }
        }

        if (!served) {
            fprintf(stderr, "client: all endpoints failed for %s\n", body);
        }
        zg_sleep_ms(200);
    }

    printf("client: done (6 requests)\n");
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "server");
    if (strcmp(role, "server") == 0) {
        server(argc, argv);
    } else if (strcmp(role, "client_sequential") == 0) {
        client_sequential(argc, argv);
    } else if (strcmp(role, "client_shotgun") == 0) {
        client_shotgun(argc, argv);
    } else if (strcmp(role, "client_tracked") == 0) {
        client_tracked(argc, argv);
    } else {
        fprintf(stderr, "usage: %s server|client_sequential|client_shotgun|client_tracked [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
