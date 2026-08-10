#include "zg.h"

static void publisher(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-02-pubsub-c");
    int count = zg_arg_int(argc, argv, 3, 20);
    void *ctx = zg_ctx();
    void *pub = zg_socket(ctx, ZMQ_PUB);

    zg_bind(pub, ep);
    printf("publisher: bound to %s\n", ep);
    zg_sleep_ms(300);

    for (int i = 0; i < count; i++) {
        char msg[80];
        snprintf(msg, sizeof(msg), "weather.nyc %dF", 55 + (i % 30));
        zg_send_str(pub, msg);
        snprintf(msg, sizeof(msg), "weather.sfo %dF", 60 + (i % 20));
        zg_send_str(pub, msg);
        snprintf(msg, sizeof(msg), "weather.chi %dF", 40 + (i % 35));
        zg_send_str(pub, msg);
        snprintf(msg, sizeof(msg), "sports.nba score-%d", i);
        zg_send_str(pub, msg);
        zg_sleep_ms(50);
    }

    printf("publisher: done (%d rounds)\n", count);
    zg_close(pub);
    zg_term(ctx);
}

static void subscriber(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-02-pubsub-c");
    const char *topic = zg_arg(argc, argv, 3, "weather.nyc");
    int count = zg_arg_int(argc, argv, 4, 10);
    void *ctx = zg_ctx();
    void *sub = zg_socket(ctx, ZMQ_SUB);

    zg_connect(sub, ep);
    zg_subscribe(sub, topic);
    printf("subscriber: connected to %s, topic=%s\n", ep, topic);

    for (int i = 0; i < count; i++) {
        char *body = zg_recv_str(sub);
        printf("subscriber[%s]: [%d] %s\n", topic, i, body);
        free(body);
    }

    printf("subscriber: done (%d messages)\n", count);
    zg_close(sub);
    zg_term(ctx);
}

static void proxy(int argc, char **argv) {
    const char *upstream_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-02-upstream-c");
    const char *downstream_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-02-downstream-c");
    void *ctx = zg_ctx();
    void *upstream = zg_socket(ctx, ZMQ_SUB);
    void *downstream = zg_socket(ctx, ZMQ_PUB);
    zg_msg msg;
    zg_msg_init(&msg);

    zg_connect(upstream, upstream_ep);
    zg_subscribe(upstream, "");
    zg_bind(downstream, downstream_ep);
    printf("proxy: upstream=%s downstream=%s\n", upstream_ep, downstream_ep);

    for (;;) {
        if (zg_msg_recv(upstream, &msg) < 0) {
            zg_die("zmq_recv");
        }
        zg_msg_send(downstream, &msg);
    }
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "publisher");
    if (strcmp(role, "publisher") == 0) {
        publisher(argc, argv);
    } else if (strcmp(role, "subscriber") == 0) {
        subscriber(argc, argv);
    } else if (strcmp(role, "proxy") == 0) {
        proxy(argc, argv);
    } else {
        fprintf(stderr, "usage: %s publisher|subscriber|proxy [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
