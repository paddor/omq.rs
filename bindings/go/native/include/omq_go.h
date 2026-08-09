#ifndef OMQ_GO_H
#define OMQ_GO_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct OmqGoContext OmqGoContext;
typedef struct OmqGoSocket OmqGoSocket;
typedef struct OmqGoMonitor OmqGoMonitor;
typedef struct OmqGoSendRing OmqGoSendRing;
typedef struct OmqGoRecvRing OmqGoRecvRing;

typedef struct {
  int32_t code;
  char *message;
} OmqGoStatus;

typedef struct {
  uint8_t *data;
  size_t len;
} OmqGoPart;

typedef struct {
  OmqGoPart *parts;
  size_t part_count;
} OmqGoMessage;

typedef struct {
  const OmqGoPart *parts;
  size_t part_count;
} OmqGoWireMessage;

typedef struct {
  char *kind;
  char *endpoint;
  char *peer_ident;
  char *reason;
  char *command_name;
  uint8_t *data;
  size_t data_len;
  uint64_t connection_id;
  uint64_t retry_millis;
  uint32_t attempt;
} OmqGoEvent;

typedef struct {
  OmqGoStatus status;
  const uint8_t *data;
  size_t len;
} OmqGoRecvView;

typedef struct {
  void *control;
  void *descriptors;
  void *payload;
  size_t desc_capacity;
  size_t payload_capacity;
} OmqGoSendRingMemory;

typedef struct {
  void *control;
  void *descriptors;
  void *payload;
  size_t desc_capacity;
  size_t payload_capacity;
} OmqGoRecvRingMemory;

enum {
  OMQ_GO_OK = 0,
  OMQ_GO_AGAIN = 1,
  OMQ_GO_CLOSED = 2,
  OMQ_GO_TIMEOUT = 3,
  OMQ_GO_CANCELED = 4,
  OMQ_GO_INVALID_ENDPOINT = 5,
  OMQ_GO_UNSUPPORTED_SCHEME = 6,
  OMQ_GO_PROTOCOL = 7,
  OMQ_GO_CONFIG = 8,
  OMQ_GO_IO = 9,
  OMQ_GO_UNROUTABLE = 10,
  OMQ_GO_MESSAGE_TOO_LARGE = 11,
  OMQ_GO_ERROR = 99
};

OmqGoStatus omq_go_context_open(size_t io_threads, OmqGoContext **out);
OmqGoStatus omq_go_context_from_share_key(uint64_t high, uint64_t low, OmqGoContext **out);
OmqGoStatus omq_go_context_share_key(OmqGoContext *ctx, uint64_t *high, uint64_t *low);
void omq_go_context_close(OmqGoContext *ctx);
void omq_go_context_free(OmqGoContext *ctx);

OmqGoStatus omq_go_socket_new(OmqGoContext *ctx, int32_t socket_type, OmqGoSocket **out);
OmqGoStatus omq_go_socket_bind(OmqGoSocket *socket, const char *endpoint, char **bound_endpoint);
OmqGoStatus omq_go_socket_connect(OmqGoSocket *socket, const char *endpoint);
OmqGoStatus omq_go_socket_unbind(OmqGoSocket *socket, const char *endpoint);
OmqGoStatus omq_go_socket_disconnect(OmqGoSocket *socket, const char *endpoint);
OmqGoStatus omq_go_socket_send(OmqGoSocket *socket, const OmqGoPart *parts, size_t part_count, int64_t timeout_millis);
OmqGoStatus omq_go_socket_send_one(OmqGoSocket *socket, const uint8_t *data, size_t len, int64_t timeout_millis);
OmqGoStatus omq_go_socket_try_send_batch(OmqGoSocket *socket, const OmqGoWireMessage *messages, size_t message_count, size_t *sent);
OmqGoStatus omq_go_socket_recv(OmqGoSocket *socket, int64_t timeout_millis, OmqGoMessage *out);
OmqGoStatus omq_go_socket_recv_one_into(OmqGoSocket *socket, int64_t timeout_millis, uint8_t *data, size_t capacity, size_t *written);
OmqGoStatus omq_go_socket_recv_one_borrow(OmqGoSocket *socket, int64_t timeout_millis, size_t capacity, const uint8_t **data, size_t *written);
OmqGoRecvView omq_go_socket_recv_one_view(OmqGoSocket *socket, int64_t timeout_millis);
OmqGoStatus omq_go_socket_clear_recv_view(OmqGoSocket *socket);
OmqGoStatus omq_go_socket_subscribe(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_unsubscribe(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_join(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_leave(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_close(OmqGoSocket *socket, int64_t linger_millis);
void omq_go_socket_free(OmqGoSocket *socket);

OmqGoStatus omq_go_socket_set_send_hwm(OmqGoSocket *socket, uint32_t value);
OmqGoStatus omq_go_socket_set_recv_hwm(OmqGoSocket *socket, uint32_t value);
OmqGoStatus omq_go_socket_set_linger(OmqGoSocket *socket, int64_t millis);
OmqGoStatus omq_go_socket_set_identity(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_set_conflate(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_router_mandatory(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_xpub_nodrop(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_compression_auto_train(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_compression_threshold(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_compression_level(OmqGoSocket *socket, int64_t level);
OmqGoStatus omq_go_socket_set_compression_dict(OmqGoSocket *socket, const uint8_t *data, size_t len);

OmqGoStatus omq_go_socket_monitor(OmqGoSocket *socket, OmqGoMonitor **out);
OmqGoStatus omq_go_monitor_recv(OmqGoMonitor *monitor, int64_t timeout_millis, OmqGoEvent *out);
void omq_go_monitor_close(OmqGoMonitor *monitor);
void omq_go_monitor_free(OmqGoMonitor *monitor);

OmqGoStatus omq_go_send_ring_create(OmqGoSocket *socket, size_t desc_capacity, size_t payload_capacity, OmqGoSendRing **out);
OmqGoStatus omq_go_send_ring_memory(OmqGoSendRing *ring, OmqGoSendRingMemory *out);
OmqGoStatus omq_go_send_ring_error(OmqGoSendRing *ring);
void omq_go_send_ring_close(OmqGoSendRing *ring);

OmqGoStatus omq_go_recv_ring_create(OmqGoSocket *socket, size_t desc_capacity, size_t payload_capacity, OmqGoRecvRing **out);
OmqGoStatus omq_go_recv_ring_memory(OmqGoRecvRing *ring, OmqGoRecvRingMemory *out);
OmqGoStatus omq_go_recv_ring_fill(OmqGoRecvRing *ring, int64_t timeout_millis, size_t max_messages);
void omq_go_recv_ring_close(OmqGoRecvRing *ring);

void omq_go_message_free(OmqGoMessage message);
void omq_go_event_free(OmqGoEvent event);
void omq_go_string_free(char *value);

#ifdef __cplusplus
}
#endif

#endif
