// Helper for libzmq draft-socket interop tests.
// Usage:
//   zmq_draft_peer radio-connect-send ENDPOINT GROUP BODY
//   zmq_draft_peer dish-connect-recv ENDPOINT GROUP BODY
//   zmq_draft_peer scatter-connect-send ENDPOINT BODY
//   zmq_draft_peer gather-connect-recv ENDPOINT BODY
//   zmq_draft_peer client-connect-request ENDPOINT REQUEST REPLY
//   zmq_draft_peer server-connect-reply ENDPOINT REQUEST REPLY
//   zmq_draft_peer channel-connect-request ENDPOINT REQUEST REPLY
//   zmq_draft_peer peer-connect-request ENDPOINT REQUEST REPLY
#define ZMQ_BUILD_DRAFT_API
#include <zmq.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void die_zmq(const char *what) {
    fprintf(stderr, "%s: %s\n", what, zmq_strerror(zmq_errno()));
    exit(1);
}

static void die_msg(const char *what) {
    fprintf(stderr, "%s\n", what);
    exit(1);
}

static void set_timeouts(void *sock) {
    int linger = 0;
    int timeout = 5000;
    if (zmq_setsockopt(sock, ZMQ_LINGER, &linger, sizeof(linger)) != 0)
        die_zmq("setsockopt linger");
    if (zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout)) != 0)
        die_zmq("setsockopt rcvtimeo");
    if (zmq_setsockopt(sock, ZMQ_SNDTIMEO, &timeout, sizeof(timeout)) != 0)
        die_zmq("setsockopt sndtimeo");
}

static void send_msg(void *sock, const char *body) {
    int rc = zmq_send(sock, body, strlen(body), 0);
    if (rc != (int)strlen(body))
        die_zmq("zmq_send");
}

static void recv_expect(void *sock, const char *expected) {
    char buf[256];
    int rc = zmq_recv(sock, buf, sizeof(buf), 0);
    if (rc < 0)
        die_zmq("zmq_recv");
    if ((size_t)rc != strlen(expected) || memcmp(buf, expected, (size_t)rc) != 0) {
        fprintf(stderr, "expected %s, got %.*s\n", expected, rc, buf);
        exit(1);
    }
}

static void radio_connect_send(void *ctx, const char *endpoint, const char *group,
                               const char *body) {
    void *sock = zmq_socket(ctx, ZMQ_RADIO);
    if (!sock)
        die_zmq("zmq_socket RADIO");
    set_timeouts(sock);
    if (zmq_connect(sock, endpoint) != 0)
        die_zmq("zmq_connect RADIO");
    zmq_sleep(1);

    zmq_msg_t msg;
    if (zmq_msg_init_size(&msg, strlen(body)) != 0)
        die_zmq("zmq_msg_init_size");
    memcpy(zmq_msg_data(&msg), body, strlen(body));
    if (zmq_msg_set_group(&msg, group) != 0)
        die_zmq("zmq_msg_set_group");
    if (zmq_msg_send(&msg, sock, 0) < 0)
        die_zmq("zmq_msg_send RADIO");
    zmq_msg_close(&msg);
    zmq_close(sock);
}

static void dish_connect_recv(void *ctx, const char *endpoint, const char *group,
                              const char *body) {
    void *sock = zmq_socket(ctx, ZMQ_DISH);
    if (!sock)
        die_zmq("zmq_socket DISH");
    set_timeouts(sock);
    if (zmq_connect(sock, endpoint) != 0)
        die_zmq("zmq_connect DISH");
    if (zmq_join(sock, group) != 0)
        die_zmq("zmq_join");

    zmq_msg_t msg;
    if (zmq_msg_init(&msg) != 0)
        die_zmq("zmq_msg_init DISH");
    if (zmq_msg_recv(&msg, sock, 0) < 0)
        die_zmq("zmq_msg_recv DISH");
    const char *got_group = zmq_msg_group(&msg);
    size_t size = zmq_msg_size(&msg);
    if (!got_group || strcmp(got_group, group) != 0 || size != strlen(body)
        || memcmp(zmq_msg_data(&msg), body, size) != 0) {
        fprintf(stderr, "expected group=%s body=%s, got group=%s body=%.*s\n",
                group, body, got_group ? got_group : "<null>", (int)size,
                (char *)zmq_msg_data(&msg));
        exit(1);
    }
    zmq_msg_close(&msg);
    zmq_close(sock);
}

static void scatter_connect_send(void *ctx, const char *endpoint, const char *body) {
    void *sock = zmq_socket(ctx, ZMQ_SCATTER);
    if (!sock)
        die_zmq("zmq_socket SCATTER");
    set_timeouts(sock);
    if (zmq_connect(sock, endpoint) != 0)
        die_zmq("zmq_connect SCATTER");
    zmq_sleep(1);
    send_msg(sock, body);
    zmq_close(sock);
}

static void gather_connect_recv(void *ctx, const char *endpoint, const char *body) {
    void *sock = zmq_socket(ctx, ZMQ_GATHER);
    if (!sock)
        die_zmq("zmq_socket GATHER");
    set_timeouts(sock);
    if (zmq_connect(sock, endpoint) != 0)
        die_zmq("zmq_connect GATHER");
    recv_expect(sock, body);
    zmq_close(sock);
}

static void request_reply(void *ctx, int socket_type, const char *socket_name,
                          const char *endpoint, const char *request,
                          const char *reply) {
    void *sock = zmq_socket(ctx, socket_type);
    if (!sock)
        die_zmq(socket_name);
    set_timeouts(sock);
    if (zmq_connect(sock, endpoint) != 0)
        die_zmq("zmq_connect");
    zmq_sleep(1);
    send_msg(sock, request);
    recv_expect(sock, reply);
    zmq_close(sock);
}

static void server_connect_reply(void *ctx, const char *endpoint, const char *request,
                                 const char *reply) {
    void *sock = zmq_socket(ctx, ZMQ_SERVER);
    if (!sock)
        die_zmq("zmq_socket SERVER");
    set_timeouts(sock);
    if (zmq_connect(sock, endpoint) != 0)
        die_zmq("zmq_connect SERVER");

    zmq_msg_t in;
    if (zmq_msg_init(&in) != 0)
        die_zmq("zmq_msg_init SERVER");
    if (zmq_msg_recv(&in, sock, 0) < 0)
        die_zmq("zmq_msg_recv SERVER");
    uint32_t routing_id = zmq_msg_routing_id(&in);
    size_t size = zmq_msg_size(&in);
    if (routing_id == 0 || size != strlen(request)
        || memcmp(zmq_msg_data(&in), request, size) != 0) {
        fprintf(stderr, "expected %s, got routing_id=%u body=%.*s\n", request,
                routing_id, (int)size, (char *)zmq_msg_data(&in));
        exit(1);
    }
    zmq_msg_close(&in);

    zmq_msg_t out;
    if (zmq_msg_init_size(&out, strlen(reply)) != 0)
        die_zmq("zmq_msg_init_size SERVER");
    memcpy(zmq_msg_data(&out), reply, strlen(reply));
    if (zmq_msg_set_routing_id(&out, routing_id) != 0)
        die_zmq("zmq_msg_set_routing_id SERVER");
    if (zmq_msg_send(&out, sock, 0) < 0)
        die_zmq("zmq_msg_send SERVER");
    zmq_msg_close(&out);
    zmq_close(sock);
}

static void peer_connect_request(void *ctx, const char *endpoint, const char *request,
                                 const char *reply) {
    void *sock = zmq_socket(ctx, ZMQ_PEER);
    if (!sock)
        die_zmq("zmq_socket PEER");
    set_timeouts(sock);
    uint32_t routing_id = zmq_connect_peer(sock, endpoint);
    if (routing_id == 0)
        die_zmq("zmq_connect_peer");
    zmq_sleep(1);

    zmq_msg_t msg;
    if (zmq_msg_init_size(&msg, strlen(request)) != 0)
        die_zmq("zmq_msg_init_size PEER");
    memcpy(zmq_msg_data(&msg), request, strlen(request));
    if (zmq_msg_set_routing_id(&msg, routing_id) != 0)
        die_zmq("zmq_msg_set_routing_id");
    if (zmq_msg_send(&msg, sock, 0) < 0)
        die_zmq("zmq_msg_send PEER");
    zmq_msg_close(&msg);

    zmq_msg_t in;
    if (zmq_msg_init(&in) != 0)
        die_zmq("zmq_msg_init");
    if (zmq_msg_recv(&in, sock, 0) < 0)
        die_zmq("zmq_msg_recv PEER");
    size_t size = zmq_msg_size(&in);
    if (size != strlen(reply) || memcmp(zmq_msg_data(&in), reply, size) != 0) {
        fprintf(stderr, "expected %s, got %.*s\n", reply, (int)size,
                (char *)zmq_msg_data(&in));
        exit(1);
    }
    zmq_msg_close(&in);
    zmq_close(sock);
}

int main(int argc, char **argv) {
    if (argc < 4)
        die_msg("usage: zmq_draft_peer MODE ENDPOINT ...");

    const char *mode = argv[1];
    const char *endpoint = argv[2];
    void *ctx = zmq_ctx_new();
    if (!ctx)
        die_zmq("zmq_ctx_new");

    if (strcmp(mode, "radio-connect-send") == 0) {
        if (argc != 5)
            die_msg("usage: radio-connect-send ENDPOINT GROUP BODY");
        radio_connect_send(ctx, endpoint, argv[3], argv[4]);
    } else if (strcmp(mode, "dish-connect-recv") == 0) {
        if (argc != 5)
            die_msg("usage: dish-connect-recv ENDPOINT GROUP BODY");
        dish_connect_recv(ctx, endpoint, argv[3], argv[4]);
    } else if (strcmp(mode, "scatter-connect-send") == 0) {
        if (argc != 4)
            die_msg("usage: scatter-connect-send ENDPOINT BODY");
        scatter_connect_send(ctx, endpoint, argv[3]);
    } else if (strcmp(mode, "gather-connect-recv") == 0) {
        if (argc != 4)
            die_msg("usage: gather-connect-recv ENDPOINT BODY");
        gather_connect_recv(ctx, endpoint, argv[3]);
    } else if (strcmp(mode, "client-connect-request") == 0) {
        if (argc != 5)
            die_msg("usage: client-connect-request ENDPOINT REQUEST REPLY");
        request_reply(ctx, ZMQ_CLIENT, "zmq_socket CLIENT", endpoint, argv[3], argv[4]);
    } else if (strcmp(mode, "server-connect-reply") == 0) {
        if (argc != 5)
            die_msg("usage: server-connect-reply ENDPOINT REQUEST REPLY");
        server_connect_reply(ctx, endpoint, argv[3], argv[4]);
    } else if (strcmp(mode, "channel-connect-request") == 0) {
        if (argc != 5)
            die_msg("usage: channel-connect-request ENDPOINT REQUEST REPLY");
        request_reply(ctx, ZMQ_CHANNEL, "zmq_socket CHANNEL", endpoint, argv[3], argv[4]);
    } else if (strcmp(mode, "peer-connect-request") == 0) {
        if (argc != 5)
            die_msg("usage: peer-connect-request ENDPOINT REQUEST REPLY");
        peer_connect_request(ctx, endpoint, argv[3], argv[4]);
    } else {
        die_msg("unknown mode");
    }

    zmq_ctx_destroy(ctx);
    return 0;
}
