#include <stdio.h>
#include <stdlib.h>
#include <zmq.h>

static void check(int result, const char *operation)
{
    if (result < 0) {
        fprintf(stderr, "%s: %s\n", operation, zmq_strerror(zmq_errno()));
        exit(EXIT_FAILURE);
    }
}

static void set_int(void *socket, int option, int value)
{
    check(zmq_setsockopt(socket, option, &value, sizeof(value)),
          "zmq_setsockopt");
}

int main(void)
{
    char server_public[41];
    char server_secret[41];
    char client_public[41];
    char client_secret[41];
    check(zmq_curve_keypair(server_public, server_secret),
          "zmq_curve_keypair server");
    check(zmq_curve_keypair(client_public, client_secret),
          "zmq_curve_keypair client");

    void *context = zmq_ctx_new();
    if (context == NULL) {
        check(-1, "zmq_ctx_new");
    }
    void *server = zmq_socket(context, ZMQ_REP);
    void *client = zmq_socket(context, ZMQ_REQ);
    if (server == NULL || client == NULL) {
        check(-1, "zmq_socket");
    }

    set_int(server, ZMQ_LINGER, 0);
    set_int(server, ZMQ_RCVTIMEO, 5000);
    set_int(client, ZMQ_LINGER, 0);
    set_int(client, ZMQ_RCVTIMEO, 5000);
    set_int(client, ZMQ_SNDTIMEO, 5000);

    set_int(server, ZMQ_CURVE_SERVER, 1);
    check(zmq_setsockopt(server, ZMQ_CURVE_SECRETKEY, server_secret, 40),
          "zmq_setsockopt server secret key");

    /* No ZAP domain is set, so any valid CURVE client key is admitted. */

    check(zmq_setsockopt(client, ZMQ_CURVE_PUBLICKEY, client_public, 40),
          "zmq_setsockopt client public key");
    check(zmq_setsockopt(client, ZMQ_CURVE_SECRETKEY, client_secret, 40),
          "zmq_setsockopt client secret key");
    check(zmq_setsockopt(client, ZMQ_CURVE_SERVERKEY, server_public, 40),
          "zmq_setsockopt server public key");

    check(zmq_bind(server, "tcp://127.0.0.1:*"), "zmq_bind");
    char endpoint[256];
    size_t endpoint_size = sizeof(endpoint);
    check(zmq_getsockopt(server, ZMQ_LAST_ENDPOINT, endpoint, &endpoint_size),
          "zmq_getsockopt");
    endpoint[sizeof(endpoint) - 1] = '\0';
    check(zmq_connect(client, endpoint), "zmq_connect");

    const char request[] = "hello over CURVE";
    check(zmq_send(client, request, sizeof(request) - 1, 0), "zmq_send");

    char received[64];
    int received_size = zmq_recv(server, received, sizeof(received) - 1, 0);
    check(received_size, "zmq_recv");
    received[received_size] = '\0';
    printf("server received: %s\n", received);

    const char reply[] = "encrypted reply";
    check(zmq_send(server, reply, sizeof(reply) - 1, 0), "zmq_send");
    received_size = zmq_recv(client, received, sizeof(received) - 1, 0);
    check(received_size, "zmq_recv");
    received[received_size] = '\0';
    printf("client received: %s\n", received);

    check(zmq_close(client), "zmq_close");
    check(zmq_close(server), "zmq_close");
    check(zmq_ctx_term(context), "zmq_ctx_term");
    return EXIT_SUCCESS;
}
