#include "zg.h"

static void publisher(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-05-heartbeat-c");
    void *ctx = zg_ctx();
    void *pub = zg_socket(ctx, ZMQ_PUB);

    zg_bind(pub, ep);
    zg_sleep_ms(100);

    for (int i = 0; i < 8; i++) {
        zg_send_str(pub, "HEARTBEAT");
        printf("publisher: heartbeat %d\n", i);
        zg_sleep_ms(50);
    }

    printf("publisher: simulating failure (300ms pause)\n");
    zg_sleep_ms(300);

    for (int i = 8; i < 16; i++) {
        zg_send_str(pub, "HEARTBEAT");
        printf("publisher: heartbeat %d\n", i);
        zg_sleep_ms(50);
    }

    printf("publisher: done\n");
    zg_close(pub);
    zg_term(ctx);
}

static void monitor(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-05-heartbeat-c");
    void *ctx = zg_ctx();
    void *sub = zg_socket(ctx, ZMQ_SUB);
    bool alive = false;

    zg_connect(sub, ep);
    zg_subscribe(sub, "HEARTBEAT");
    zg_sleep_ms(50);

    for (int i = 0; i < 20; i++) {
        char *body = zg_recv_str_timeout(sub, 150);
        if (body != NULL) {
            if (!alive) {
                alive = true;
                printf("monitor: ALIVE (%s)\n", body);
            }
            free(body);
        } else if (alive) {
            alive = false;
            printf("monitor: DEAD (timeout)\n");
        }
    }

    printf("monitor: done\n");
    zg_close(sub);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "monitor");
    if (strcmp(role, "publisher") == 0) {
        publisher(argc, argv);
    } else if (strcmp(role, "monitor") == 0) {
        monitor(argc, argv);
    } else {
        fprintf(stderr, "usage: %s publisher|monitor [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
