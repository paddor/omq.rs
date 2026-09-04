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
    const omq_plain_credential_t credentials[] = {
        {(const uint8_t *) "alice", 5, (const uint8_t *) "wonderland", 10},
        {(const uint8_t *) "bob", 3, (const uint8_t *) "builder", 7},
        {(const uint8_t *) "carol", 5, (const uint8_t *) "s3cret", 6},
    };
    const size_t credential_count =
        sizeof(credentials) / sizeof(credentials[0]);

    void *context = zmq_ctx_new();
    if (context == NULL) {
        check(-1, "zmq_ctx_new");
    }
    void *server = zmq_socket(context, ZMQ_PULL);
    if (server == NULL) {
        check(-1, "zmq_socket");
    }

    set_int(server, ZMQ_LINGER, 0);
    set_int(server, ZMQ_RCVTIMEO, 5000);
    check(omq_socket_set_plain_server_credentials(
              server, credentials, credential_count),
          "omq_socket_set_plain_server_credentials");
    check(zmq_bind(server, "tcp://127.0.0.1:*"), "zmq_bind");

    char endpoint[256];
    size_t endpoint_size = sizeof(endpoint);
    check(zmq_getsockopt(server, ZMQ_LAST_ENDPOINT, endpoint, &endpoint_size),
          "zmq_getsockopt");
    endpoint[sizeof(endpoint) - 1] = '\0';

    for (size_t index = 0; index < credential_count; index++) {
        const omq_plain_credential_t *credential = &credentials[index];
        void *client = zmq_socket(context, ZMQ_PUSH);
        if (client == NULL) {
            check(-1, "zmq_socket");
        }
        set_int(client, ZMQ_LINGER, 0);
        set_int(client, ZMQ_SNDTIMEO, 5000);
        check(zmq_setsockopt(client, ZMQ_PLAIN_USERNAME,
                             credential->username, credential->username_size),
              "zmq_setsockopt username");
        check(zmq_setsockopt(client, ZMQ_PLAIN_PASSWORD,
                             credential->password, credential->password_size),
              "zmq_setsockopt password");
        check(zmq_connect(client, endpoint), "zmq_connect");

        char sent[64];
        int sent_size = snprintf(sent, sizeof(sent), "hello from %.*s",
                                 (int) credential->username_size,
                                 (const char *) credential->username);
        if (sent_size < 0 || (size_t) sent_size >= sizeof(sent)) {
            fprintf(stderr, "message formatting failed\n");
            exit(EXIT_FAILURE);
        }
        check(zmq_send(client, sent, (size_t) sent_size, 0), "zmq_send");

        char received[64];
        int received_size =
            zmq_recv(server, received, sizeof(received) - 1, 0);
        check(received_size, "zmq_recv");
        received[received_size] = '\0';
        printf("accepted: %s\n", received);

        check(zmq_close(client), "zmq_close");
    }

    check(zmq_close(server), "zmq_close");
    check(zmq_ctx_term(context), "zmq_ctx_term");
    return EXIT_SUCCESS;
}
