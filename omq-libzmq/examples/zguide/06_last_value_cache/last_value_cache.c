#include "zg.h"

typedef struct {
    char topic[64];
    char value[128];
} cache_entry;

static cache_entry CACHE[32];
static int CACHE_LEN = 0;

static void cache_set(const char *topic, const char *value) {
    for (int i = 0; i < CACHE_LEN; i++) {
        if (strcmp(CACHE[i].topic, topic) == 0) {
            snprintf(CACHE[i].value, sizeof(CACHE[i].value), "%s", value);
            return;
        }
    }
    if (CACHE_LEN < 32) {
        snprintf(CACHE[CACHE_LEN].topic, sizeof(CACHE[CACHE_LEN].topic), "%s", topic);
        snprintf(CACHE[CACHE_LEN].value, sizeof(CACHE[CACHE_LEN].value), "%s", value);
        CACHE_LEN++;
    }
}

static char *cache_snapshot(void) {
    char *out = zg_strdup("");
    size_t used = 0;
    for (int i = 0; i < CACHE_LEN; i++) {
        char *line = zg_printf_alloc("%s %s\n", CACHE[i].topic, CACHE[i].value);
        size_t add = strlen(line);
        char *next = (char *)realloc(out, used + add + 1);
        if (next == NULL) {
            perror("realloc");
            exit(1);
        }
        out = next;
        memcpy(out + used, line, add + 1);
        used += add;
        free(line);
    }
    if (used > 0 && out[used - 1] == '\n') {
        out[used - 1] = 0;
    }
    return out;
}

static void cache(int argc, char **argv) {
    const char *pub_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-06-publisher-c");
    const char *sub_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-06-subscriber-c");
    const char *snapshot_ep = zg_arg(argc, argv, 4, "ipc://@omq-zguide-06-snapshot-c");
    void *ctx = zg_ctx();
    void *pull = zg_socket(ctx, ZMQ_PULL);
    void *pub = zg_socket(ctx, ZMQ_PUB);
    void *rep = zg_socket(ctx, ZMQ_REP);

    zg_bind(pull, pub_ep);
    zg_bind(pub, sub_ep);
    zg_bind(rep, snapshot_ep);
    printf("cache: PULL bound to %s\n", pub_ep);
    printf("cache: PUB  bound to %s\n", sub_ep);
    printf("cache: REP  bound to %s\n", snapshot_ep);

    zmq_pollitem_t items[] = {
        {pull, 0, ZMQ_POLLIN, 0},
        {rep, 0, ZMQ_POLLIN, 0},
    };

    for (;;) {
        zg_check(zmq_poll(items, 2, -1), "zmq_poll");
        if (items[0].revents & ZMQ_POLLIN) {
            char *body = zg_recv_str(pull);
            char topic[64];
            char value[128];
            if (sscanf(body, "%63s %127[^\n]", topic, value) == 2) {
                cache_set(topic, value);
                printf("cache: cached %s=%s\n", topic, value);
            }
            zg_send_str(pub, body);
            free(body);
        }
        if (items[1].revents & ZMQ_POLLIN) {
            char *body = zg_recv_str(rep);
            if (strcmp(body, "SNAPSHOT") == 0) {
                char *snapshot = cache_snapshot();
                printf("cache: snapshot served (%d entries)\n", CACHE_LEN);
                zg_send_str(rep, snapshot);
                free(snapshot);
            } else {
                zg_send_str(rep, "");
            }
            free(body);
        }
    }
}

static void publisher(int argc, char **argv) {
    const char *ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-06-publisher-c");
    int count = zg_arg_int(argc, argv, 3, 5);
    void *ctx = zg_ctx();
    void *push = zg_socket(ctx, ZMQ_PUSH);

    zg_connect(push, ep);
    printf("publisher: connected to %s, sending %d rounds\n", ep, count);
    zg_sleep_ms(100);

    for (int i = 0; i < count; i++) {
        char nyc[64];
        char sfo[64];
        snprintf(nyc, sizeof(nyc), "weather.nyc %dF", 70 + i);
        snprintf(sfo, sizeof(sfo), "weather.sfo %dF", 60 + i);
        zg_send_str(push, nyc);
        zg_send_str(push, sfo);
        printf("publisher: %s, %s\n", nyc, sfo);
        zg_sleep_ms(50);
    }

    printf("publisher: done (%d rounds)\n", count);
    zg_close(push);
    zg_term(ctx);
}

static void subscriber(int argc, char **argv) {
    const char *snapshot_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-06-snapshot-c");
    const char *sub_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-06-subscriber-c");
    void *ctx = zg_ctx();
    void *req = zg_socket(ctx, ZMQ_REQ);
    void *sub = zg_socket(ctx, ZMQ_SUB);

    zg_connect(req, snapshot_ep);
    zg_sleep_ms(50);
    zg_send_str(req, "SNAPSHOT");
    char *snapshot = zg_recv_str(req);
    printf("subscriber: snapshot from cache:\n");
    if (snapshot[0] == 0) {
        printf("  (empty)\n");
    } else {
        char *line = strtok(snapshot, "\n");
        while (line != NULL) {
            printf("  %s\n", line);
            line = strtok(NULL, "\n");
        }
    }
    free(snapshot);

    zg_connect(sub, sub_ep);
    zg_subscribe(sub, "");
    printf("subscriber: listening for live updates (2s) ...\n");
    int64_t end = zg_now_ms() + 2000;
    while (zg_now_ms() < end) {
        char *body = zg_recv_str_timeout(sub, 200);
        if (body != NULL) {
            printf("  live: %s\n", body);
            free(body);
        }
    }

    printf("subscriber: done\n");
    zg_close(sub);
    zg_close(req);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "cache");
    if (strcmp(role, "cache") == 0) {
        cache(argc, argv);
    } else if (strcmp(role, "publisher") == 0) {
        publisher(argc, argv);
    } else if (strcmp(role, "subscriber") == 0) {
        subscriber(argc, argv);
    } else {
        fprintf(stderr, "usage: %s cache|publisher|subscriber [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
