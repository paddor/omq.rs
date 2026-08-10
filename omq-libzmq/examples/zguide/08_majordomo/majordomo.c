#include "zg.h"

typedef struct {
    char *data;
    size_t len;
} frame_ref;

typedef struct {
    char name[32];
    frame_ref workers[16];
    int count;
} service_pool;

static service_pool SERVICES[8];
static int SERVICE_LEN = 0;

static service_pool *service_get(const char *name) {
    for (int i = 0; i < SERVICE_LEN; i++) {
        if (strcmp(SERVICES[i].name, name) == 0) {
            return &SERVICES[i];
        }
    }
    if (SERVICE_LEN == 8) {
        return NULL;
    }
    snprintf(SERVICES[SERVICE_LEN].name, sizeof(SERVICES[SERVICE_LEN].name), "%s", name);
    return &SERVICES[SERVICE_LEN++];
}

static void service_push(const char *service, const char *id, size_t len) {
    service_pool *pool = service_get(service);
    if (pool == NULL || pool->count == 16) {
        return;
    }
    pool->workers[pool->count].data = zg_strndup(id, len);
    pool->workers[pool->count].len = len;
    pool->count++;
}

static frame_ref service_pop(const char *service) {
    frame_ref out = {0};
    service_pool *pool = service_get(service);
    if (pool == NULL || pool->count == 0) {
        return out;
    }
    out = pool->workers[0];
    for (int i = 1; i < pool->count; i++) {
        pool->workers[i - 1] = pool->workers[i];
    }
    pool->count--;
    return out;
}

static void broker(int argc, char **argv) {
    const char *frontend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-08-frontend-c");
    const char *backend_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-08-backend-c");
    int n_workers = zg_arg_int(argc, argv, 4, 3);
    void *ctx = zg_ctx();
    void *frontend = zg_socket(ctx, ZMQ_ROUTER);
    void *backend = zg_socket(ctx, ZMQ_ROUTER);
    zg_msg msg;
    zg_msg_init(&msg);

    zg_bind(frontend, frontend_ep);
    zg_bind(backend, backend_ep);
    printf("broker: frontend=%s backend=%s n_workers=%d\n", frontend_ep, backend_ep, n_workers);

    for (int i = 0; i < n_workers; i++) {
        if (zg_msg_recv(backend, &msg) < 0) {
            zg_die("broker recv worker");
        }
        if (msg.count >= 3 && strcmp(msg.data[1], "READY") == 0) {
            service_push(msg.data[2], msg.data[0], msg.size[0]);
            printf("broker: worker '%s' registered for '%s'\n", msg.data[0], msg.data[2]);
        }
    }

    for (;;) {
        if (zg_msg_recv(frontend, &msg) < 0) {
            zg_die("broker recv client");
        }
        if (msg.count < 4) {
            zg_msg_clear(&msg);
            continue;
        }

        frame_ref worker = service_pop(msg.data[2]);
        if (worker.data == NULL) {
            printf("broker: no worker for service '%s'\n", msg.data[2]);
            zg_msg_clear(&msg);
            continue;
        }

        printf("broker: routing '%s' request to %s\n", msg.data[2], worker.data);
        zg_send_data(backend, worker.data, worker.len, 1);
        zg_send_data(backend, msg.data[0], msg.size[0], 1);
        zg_send_more(backend, "");
        zg_send_data(backend, msg.data[3], msg.size[3], 0);
        free(worker.data);

        zg_msg reply;
        zg_msg_init(&reply);
        if (zg_msg_recv(backend, &reply) < 0) {
            zg_die("broker recv reply");
        }
        if (reply.count >= 4) {
            service_push(msg.data[2], reply.data[0], reply.size[0]);
            zg_send_data(frontend, reply.data[1], reply.size[1], 1);
            zg_send_more(frontend, "");
            zg_send_data(frontend, reply.data[3], reply.size[3], 0);
        }
        zg_msg_clear(&reply);
        zg_msg_clear(&msg);
    }
}

static void worker(int argc, char **argv) {
    const char *backend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-08-backend-c");
    const char *service = zg_arg(argc, argv, 3, "echo");
    const char *id = zg_arg(argc, argv, 4, "0");
    char identity[64];
    snprintf(identity, sizeof(identity), "%s-%s", service, id);
    void *ctx = zg_ctx();
    void *dealer = zg_socket(ctx, ZMQ_DEALER);

    zg_check(zmq_setsockopt(dealer, ZMQ_IDENTITY, identity, strlen(identity)), "ZMQ_IDENTITY");
    zg_connect(dealer, backend_ep);
    zg_send_more(dealer, "READY");
    zg_send_str(dealer, service);
    printf("worker(%s): ready\n", identity);

    for (;;) {
        zg_msg msg;
        zg_msg_init(&msg);
        if (zg_msg_recv(dealer, &msg) < 0) {
            zg_die("worker recv");
        }
        if (msg.count < 3) {
            zg_msg_clear(&msg);
            continue;
        }
        char *reply = NULL;
        if (strcmp(service, "echo") == 0) {
            reply = zg_printf_alloc("echo:%s", msg.data[2]);
        } else if (strcmp(service, "upper") == 0) {
            reply = zg_strdup(msg.data[2]);
            zg_upper(reply);
        } else {
            reply = zg_strdup(msg.data[2]);
        }
        printf("worker(%s): %s -> %s\n", identity, msg.data[2], reply);
        zg_send_data(dealer, msg.data[0], msg.size[0], 1);
        zg_send_more(dealer, "");
        zg_send_str(dealer, reply);
        free(reply);
        zg_msg_clear(&msg);
    }
}

static void client(int argc, char **argv) {
    const char *frontend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-08-frontend-c");
    const char *requests[][2] = {
        {"echo", "hello"},
        {"echo", "world"},
        {"upper", "foo"},
        {"echo", "test"},
        {"upper", "bar"},
        {"upper", "baz"},
    };
    void *ctx = zg_ctx();
    void *req = zg_socket(ctx, ZMQ_REQ);
    zg_connect(req, frontend_ep);
    zg_sleep_ms(100);

    for (size_t i = 0; i < sizeof(requests) / sizeof(requests[0]); i++) {
        zg_send_more(req, requests[i][0]);
        zg_send_str(req, requests[i][1]);
        char *reply = zg_recv_str(req);
        printf("client: %s(%s) -> %s\n", requests[i][0], requests[i][1], reply);
        free(reply);
    }
    printf("done: %zu requests\n", sizeof(requests) / sizeof(requests[0]));
    zg_close(req);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "client");
    if (strcmp(role, "broker") == 0) {
        broker(argc, argv);
    } else if (strcmp(role, "worker") == 0) {
        worker(argc, argv);
    } else if (strcmp(role, "client") == 0) {
        client(argc, argv);
    } else {
        fprintf(stderr, "usage: %s broker|worker|client [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
