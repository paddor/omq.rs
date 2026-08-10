#include "zg.h"

typedef struct {
    char key[64];
    char value[128];
    uint64_t seq;
} kv_entry;

typedef struct {
    pthread_mutex_t lock;
    kv_entry entries[32];
    int len;
    int running;
    void *rep;
} clone_store;

static void store_set(clone_store *store, const char *key, const char *value, uint64_t seq) {
    pthread_mutex_lock(&store->lock);
    for (int i = 0; i < store->len; i++) {
        if (strcmp(store->entries[i].key, key) == 0) {
            snprintf(store->entries[i].value, sizeof(store->entries[i].value), "%s", value);
            store->entries[i].seq = seq;
            pthread_mutex_unlock(&store->lock);
            return;
        }
    }
    if (store->len < 32) {
        snprintf(store->entries[store->len].key, sizeof(store->entries[store->len].key), "%s", key);
        snprintf(store->entries[store->len].value, sizeof(store->entries[store->len].value), "%s", value);
        store->entries[store->len].seq = seq;
        store->len++;
    }
    pthread_mutex_unlock(&store->lock);
}

static char *store_snapshot(clone_store *store, int *count) {
    pthread_mutex_lock(&store->lock);
    char *out = zg_strdup("");
    size_t used = 0;
    *count = store->len;
    for (int i = 0; i < store->len; i++) {
        char line[256];
        snprintf(line, sizeof(line), "%" PRIu64 "|%s|%s\n", store->entries[i].seq, store->entries[i].key, store->entries[i].value);
        size_t add = strlen(line);
        char *next = (char *)realloc(out, used + add + 1);
        if (next == NULL) {
            perror("realloc");
            exit(1);
        }
        out = next;
        memcpy(out + used, line, add + 1);
        used += add;
    }
    if (used > 0 && out[used - 1] == '\n') {
        out[used - 1] = 0;
    }
    pthread_mutex_unlock(&store->lock);
    return out;
}

static void *snapshot_thread(void *arg) {
    clone_store *store = (clone_store *)arg;
    zg_set_i32(store->rep, ZMQ_RCVTIMEO, 100);
    while (store->running) {
        char *body = zg_recv_str(store->rep);
        if (body == NULL) {
            if (zmq_errno() == EAGAIN) {
                continue;
            }
            zg_die("snapshot recv");
        }
        if (strcmp(body, "SNAPSHOT") == 0) {
            int count = 0;
            char *payload = store_snapshot(store, &count);
            printf("server: snapshot served (%d entries)\n", count);
            zg_send_str(store->rep, payload);
            free(payload);
        } else {
            zg_send_str(store->rep, "");
        }
        free(body);
    }
    return NULL;
}

static void server(int argc, char **argv) {
    const char *updates_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-07-updates-c");
    const char *snapshot_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-07-snapshot-c");
    void *ctx = zg_ctx();
    void *pub = zg_socket(ctx, ZMQ_PUB);
    void *rep = zg_socket(ctx, ZMQ_REP);
    clone_store store;
    memset(&store, 0, sizeof(store));
    pthread_mutex_init(&store.lock, NULL);
    store.running = 1;
    store.rep = rep;

    zg_bind(pub, updates_ep);
    zg_bind(rep, snapshot_ep);
    printf("server: PUB bound to %s\n", updates_ep);
    printf("server: REP bound to %s\n", snapshot_ep);

    pthread_t thread;
    pthread_create(&thread, NULL, snapshot_thread, &store);
    zg_sleep_ms(200);

    uint64_t seq = 0;
    for (int i = 0; i < 5; i++) {
        seq++;
        char key[32];
        char value[32];
        char msg[128];
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(value, sizeof(value), "val-%d", i);
        store_set(&store, key, value, seq);
        snprintf(msg, sizeof(msg), "%" PRIu64 "|%s|%s", seq, key, value);
        zg_send_str(pub, msg);
        printf("server: published %s=%s (seq=%" PRIu64 ")\n", key, value, seq);
        zg_sleep_ms(20);
    }

    zg_sleep_ms(300);

    for (int i = 0; i < 3; i++) {
        seq++;
        char key[32];
        char value[32];
        char msg[128];
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(value, sizeof(value), "updated-%d", i);
        store_set(&store, key, value, seq);
        snprintf(msg, sizeof(msg), "%" PRIu64 "|%s|%s", seq, key, value);
        zg_send_str(pub, msg);
        printf("server: published %s=%s (seq=%" PRIu64 ")\n", key, value, seq);
        zg_sleep_ms(20);
    }

    zg_sleep_ms(3000);
    store.running = 0;
    pthread_join(thread, NULL);
    printf("server: done\n");
    zg_close(rep);
    zg_close(pub);
    zg_term(ctx);
}

static void client_set(kv_entry *entries, int *len, const char *key, const char *value, uint64_t seq) {
    for (int i = 0; i < *len; i++) {
        if (strcmp(entries[i].key, key) == 0) {
            snprintf(entries[i].value, sizeof(entries[i].value), "%s", value);
            entries[i].seq = seq;
            return;
        }
    }
    if (*len < 32) {
        snprintf(entries[*len].key, sizeof(entries[*len].key), "%s", key);
        snprintf(entries[*len].value, sizeof(entries[*len].value), "%s", value);
        entries[*len].seq = seq;
        (*len)++;
    }
}

static void apply_line(kv_entry *entries, int *len, uint64_t *snapshot_seq, const char *line, bool live) {
    uint64_t seq = 0;
    char key[64];
    char value[128];
    if (sscanf(line, "%" SCNu64 "|%63[^|]|%127[^\n]", &seq, key, value) != 3) {
        return;
    }
    if (!live || seq > *snapshot_seq) {
        client_set(entries, len, key, value, seq);
        if (!live && seq > *snapshot_seq) {
            *snapshot_seq = seq;
        }
        printf("client (%s): %s=%s seq=%" PRIu64 "\n", live ? "live" : "snapshot", key, value, seq);
    } else {
        printf("client (skip): %s=%s seq=%" PRIu64 " (already in snapshot)\n", key, value, seq);
    }
}

static void client(int argc, char **argv) {
    const char *updates_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-07-updates-c");
    const char *snapshot_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-07-snapshot-c");
    void *ctx = zg_ctx();
    void *sub = zg_socket(ctx, ZMQ_SUB);
    void *req = zg_socket(ctx, ZMQ_REQ);
    kv_entry entries[32];
    int len = 0;
    uint64_t snapshot_seq = 0;

    zg_connect(sub, updates_ep);
    zg_subscribe(sub, "");
    printf("client: SUB connected to %s\n", updates_ep);
    zg_sleep_ms(100);

    zg_connect(req, snapshot_ep);
    zg_sleep_ms(50);
    zg_send_str(req, "SNAPSHOT");
    char *snapshot = zg_recv_str(req);
    char *line = strtok(snapshot, "\n");
    while (line != NULL) {
        apply_line(entries, &len, &snapshot_seq, line, false);
        line = strtok(NULL, "\n");
    }
    free(snapshot);
    printf("client: snapshot has %d entries (up to seq=%" PRIu64 ")\n", len, snapshot_seq);

    int64_t end = zg_now_ms() + 3000;
    while (zg_now_ms() < end) {
        char *body = zg_recv_str_timeout(sub, 200);
        if (body != NULL) {
            apply_line(entries, &len, &snapshot_seq, body, true);
            free(body);
        }
    }

    printf("client: final store (%d entries):\n", len);
    for (int i = 0; i < len; i++) {
        printf("  %s = %s\n", entries[i].key, entries[i].value);
    }
    printf("client: done\n");
    zg_close(req);
    zg_close(sub);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "server");
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
