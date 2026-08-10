#include "zg.h"

typedef struct {
    void *pub;
} heartbeat_args;

static void *heartbeat_thread(void *arg) {
    heartbeat_args *args = (heartbeat_args *)arg;
    for (;;) {
        zg_send_str(args->pub, "HB");
        zg_sleep_ms(50);
    }
    return NULL;
}

static void primary(int argc, char **argv) {
    const char *service_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-10-primary-c");
    const char *heartbeat_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-10-heartbeat-c");
    void *ctx = zg_ctx();
    void *rep = zg_socket(ctx, ZMQ_REP);
    void *pub = zg_socket(ctx, ZMQ_PUB);
    heartbeat_args args = {.pub = pub};
    pthread_t thread;

    zg_bind(rep, service_ep);
    zg_bind(pub, heartbeat_ep);
    pthread_create(&thread, NULL, heartbeat_thread, &args);

    for (;;) {
        char *body = zg_recv_str(rep);
        char *reply = zg_printf_alloc("primary:%s", body);
        printf("primary: served %s\n", body);
        zg_send_str(rep, reply);
        free(body);
        free(reply);
    }
}

static void backup(int argc, char **argv) {
    const char *heartbeat_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-10-heartbeat-c");
    const char *service_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-10-backup-c");
    void *ctx = zg_ctx();
    void *sub = zg_socket(ctx, ZMQ_SUB);
    void *rep = zg_socket(ctx, ZMQ_REP);

    zg_connect(sub, heartbeat_ep);
    zg_subscribe(sub, "HB");
    zg_bind(rep, service_ep);
    zg_sleep_ms(100);

    for (;;) {
        char *hb = zg_recv_str_timeout(sub, 500);
        if (hb == NULL) {
            printf("backup: primary heartbeat lost -- taking over!\n");
            break;
        }
        free(hb);
    }

    for (;;) {
        char *body = zg_recv_str(rep);
        char *reply = zg_printf_alloc("backup:%s", body);
        printf("backup: served %s\n", body);
        zg_send_str(rep, reply);
        free(body);
        free(reply);
    }
}

static char *request_once(void *ctx, const char *ep, const char *body, int timeout_ms) {
    void *req = zg_socket(ctx, ZMQ_REQ);
    zg_connect(req, ep);
    zg_sleep_ms(20);
    zg_send_str(req, body);
    char *reply = zg_recv_str_timeout(req, timeout_ms);
    zg_close(req);
    return reply;
}

static void client(int argc, char **argv) {
    const char *primary_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-10-primary-c");
    const char *backup_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-10-backup-c");
    int n = zg_arg_int(argc, argv, 4, 4);
    void *ctx = zg_ctx();

    for (int i = 0; i < n; i++) {
        char body[32];
        snprintf(body, sizeof(body), "req-%d", i);
        char *reply = request_once(ctx, primary_ep, body, 200);
        if (reply != NULL) {
            printf("client: %s -> %s\n", body, reply);
            free(reply);
            continue;
        }
        printf("client: primary timeout for %s, trying backup\n", body);
        reply = request_once(ctx, backup_ep, body, 1000);
        if (reply != NULL) {
            printf("client: %s -> %s\n", body, reply);
            free(reply);
        } else {
            fprintf(stderr, "client: backup also timed out for %s\n", body);
        }
    }

    printf("client: done (%d requests)\n", n);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "client");
    if (strcmp(role, "primary") == 0) {
        primary(argc, argv);
    } else if (strcmp(role, "backup") == 0) {
        backup(argc, argv);
    } else if (strcmp(role, "client") == 0) {
        client(argc, argv);
    } else {
        fprintf(stderr, "usage: %s primary|backup|client [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
