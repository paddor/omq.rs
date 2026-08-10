#include "zg.h"

#include <sys/stat.h>
#include <sys/types.h>

static void ensure_dir(const char *path) {
    if (mkdir(path, 0700) != 0 && errno != EEXIST) {
        perror("mkdir");
        exit(1);
    }
}

static void write_text(const char *path, const char *text) {
    FILE *f = fopen(path, "w");
    if (f == NULL) {
        perror(path);
        exit(1);
    }
    fputs(text, f);
    fclose(f);
}

static char *read_text(const char *path) {
    FILE *f = fopen(path, "r");
    if (f == NULL) {
        return NULL;
    }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (n < 0) {
        fclose(f);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)n + 1);
    if (buf == NULL) {
        perror("malloc");
        exit(1);
    }
    size_t got = fread(buf, 1, (size_t)n, f);
    buf[got] = 0;
    fclose(f);
    return buf;
}

static void frontend(int argc, char **argv) {
    const char *frontend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-09-frontend-c");
    const char *dispatch_ep = zg_arg(argc, argv, 3, "ipc://@omq-zguide-09-dispatch-c");
    const char *store_dir = zg_arg(argc, argv, 4, "/tmp/omq-titanic-c");
    void *ctx = zg_ctx();
    void *rep = zg_socket(ctx, ZMQ_REP);
    void *push = zg_socket(ctx, ZMQ_PUSH);
    uint64_t next = 1;

    ensure_dir(store_dir);
    zg_bind(rep, frontend_ep);
    zg_bind(push, dispatch_ep);
    zg_set_i32(rep, ZMQ_RCVTIMEO, 3000);
    printf("frontend: %s dispatch=%s store=%s\n", frontend_ep, dispatch_ep, store_dir);

    for (;;) {
        char *body = zg_recv_str(rep);
        if (body == NULL) {
            if (zmq_errno() == EAGAIN) {
                break;
            }
            zg_die("frontend recv");
        }

        char *cmd = strtok(body, "|");
        char *arg1 = strtok(NULL, "|");
        char *arg2 = strtok(NULL, "");

        if (cmd != NULL && strcmp(cmd, "SUBMIT") == 0 && arg1 != NULL && arg2 != NULL) {
            char ticket[32];
            char req_path[512];
            char contents[512];
            snprintf(ticket, sizeof(ticket), "%016" PRIx64, next++);
            snprintf(req_path, sizeof(req_path), "%s/%s.req", store_dir, ticket);
            snprintf(contents, sizeof(contents), "%s|%s", arg1, arg2);
            write_text(req_path, contents);

            char *reply = zg_printf_alloc("TICKET|%s", ticket);
            zg_send_str(rep, reply);
            zg_send_str(push, ticket);
            printf("frontend: accepted %s for '%s'\n", ticket, arg1);
            free(reply);
        } else if (cmd != NULL && strcmp(cmd, "RESULT") == 0 && arg1 != NULL) {
            char res_path[512];
            snprintf(res_path, sizeof(res_path), "%s/%s.res", store_dir, arg1);
            char *contents = read_text(res_path);
            if (contents != NULL) {
                char *reply = zg_printf_alloc("OK|%s", contents);
                zg_send_str(rep, reply);
                printf("frontend: served result for %s\n", arg1);
                free(reply);
                free(contents);
            } else {
                zg_send_str(rep, "PENDING");
            }
        } else {
            zg_send_str(rep, "ERROR|unknown command");
        }
        free(body);
    }

    printf("frontend: done (recv timeout)\n");
    zg_close(push);
    zg_close(rep);
    zg_term(ctx);
}

static void dispatcher(int argc, char **argv) {
    const char *dispatch_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-09-dispatch-c");
    const char *store_dir = zg_arg(argc, argv, 3, "/tmp/omq-titanic-c");
    void *ctx = zg_ctx();
    void *pull = zg_socket(ctx, ZMQ_PULL);

    zg_connect(pull, dispatch_ep);
    zg_set_i32(pull, ZMQ_RCVTIMEO, 3000);
    printf("dispatcher: %s store=%s\n", dispatch_ep, store_dir);

    for (;;) {
        char *ticket = zg_recv_str(pull);
        if (ticket == NULL) {
            if (zmq_errno() == EAGAIN) {
                break;
            }
            zg_die("dispatcher recv");
        }
        char req_path[512];
        snprintf(req_path, sizeof(req_path), "%s/%s.req", store_dir, ticket);
        char *contents = read_text(req_path);
        if (contents == NULL) {
            free(ticket);
            continue;
        }

        char *service = strtok(contents, "|");
        char *body = strtok(NULL, "");
        if (body == NULL) {
            body = "";
        }

        char *result = NULL;
        if (service != NULL && strcmp(service, "echo") == 0) {
            result = zg_printf_alloc("echo:%s", body);
        } else if (service != NULL && strcmp(service, "upper") == 0) {
            result = zg_strdup(body);
            zg_upper(result);
        } else {
            result = zg_printf_alloc("unknown service: %s", service == NULL ? "" : service);
        }

        char res_path[512];
        snprintf(res_path, sizeof(res_path), "%s/%s.res", store_dir, ticket);
        write_text(res_path, result);
        printf("dispatcher: processed %s -> %s\n", ticket, result);
        free(result);
        free(contents);
        free(ticket);
    }

    printf("dispatcher: done (recv timeout)\n");
    zg_close(pull);
    zg_term(ctx);
}

static void client(int argc, char **argv) {
    const char *frontend_ep = zg_arg(argc, argv, 2, "ipc://@omq-zguide-09-frontend-c");
    const char *requests[][2] = {
        {"echo", "hello"},
        {"upper", "world"},
        {"echo", "foo"},
    };
    char *tickets[3] = {0};
    void *ctx = zg_ctx();
    void *req = zg_socket(ctx, ZMQ_REQ);

    zg_connect(req, frontend_ep);
    zg_sleep_ms(100);

    for (int i = 0; i < 3; i++) {
        char *body = zg_printf_alloc("SUBMIT|%s|%s", requests[i][0], requests[i][1]);
        zg_send_str(req, body);
        free(body);
        char *reply = zg_recv_str(req);
        char *status = strtok(reply, "|");
        char *ticket = strtok(NULL, "");
        if (status != NULL && strcmp(status, "TICKET") == 0 && ticket != NULL) {
            tickets[i] = zg_strdup(ticket);
            printf("client: submitted %s(%s) -> ticket %s\n", requests[i][0], requests[i][1], tickets[i]);
        }
        free(reply);
    }

    zg_sleep_ms(500);

    for (int i = 0; i < 3; i++) {
        char *body = zg_printf_alloc("RESULT|%s", tickets[i]);
        zg_send_str(req, body);
        free(body);
        char *reply = zg_recv_str(req);
        char *status = strtok(reply, "|");
        char *result = strtok(NULL, "");
        if (status != NULL && strcmp(status, "OK") == 0 && result != NULL) {
            printf("client: result for %s -> %s\n", tickets[i], result);
        } else {
            printf("client: result for %s -> %s\n", tickets[i], reply);
        }
        free(reply);
        free(tickets[i]);
    }

    printf("done: 3 requests persisted, dispatched, and retrieved\n");
    zg_close(req);
    zg_term(ctx);
}

int main(int argc, char **argv) {
    const char *role = zg_arg(argc, argv, 1, "client");
    if (strcmp(role, "frontend") == 0) {
        frontend(argc, argv);
    } else if (strcmp(role, "dispatcher") == 0) {
        dispatcher(argc, argv);
    } else if (strcmp(role, "client") == 0) {
        client(argc, argv);
    } else {
        fprintf(stderr, "usage: %s frontend|dispatcher|client [args...]\n", argv[0]);
        return 2;
    }
    return 0;
}
