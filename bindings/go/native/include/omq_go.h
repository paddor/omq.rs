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
typedef struct OmqGoCancel OmqGoCancel;

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
  char *peer_address;
  char *peer_socket_type;
  char *reason;
  char *command_name;
  uint8_t *data;
  size_t data_len;
  uint8_t *peer_identity;
  size_t peer_identity_len;
  uint64_t connection_id;
  uint64_t retry_millis;
  uint32_t attempt;
  uint32_t zmtp_major;
  uint32_t zmtp_minor;
  int has_peer;
} OmqGoEvent;

typedef struct {
  const uint8_t *mechanism_data;
  size_t mechanism_len;
  const uint8_t *public_key_data;
  size_t public_key_len;
  const uint8_t *identity_data;
  size_t identity_len;
  const uint8_t *username_data;
  size_t username_len;
  const uint8_t *password_data;
  size_t password_len;
} OmqGoAuthPeer;

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

OmqGoStatus omq_go_curve_keypair(char **public_key, char **secret_key);
OmqGoStatus omq_go_curve_public(const char *secret_key, char **public_key);

OmqGoStatus omq_go_socket_new(OmqGoContext *ctx, int32_t socket_type, OmqGoSocket **out);
OmqGoStatus omq_go_socket_bind(OmqGoSocket *socket, const char *endpoint, char **bound_endpoint);
OmqGoStatus omq_go_socket_connect(OmqGoSocket *socket, const char *endpoint);
OmqGoStatus omq_go_socket_unbind(OmqGoSocket *socket, const char *endpoint);
OmqGoStatus omq_go_socket_disconnect(OmqGoSocket *socket, const char *endpoint);
OmqGoStatus omq_go_socket_send(OmqGoSocket *socket, const OmqGoPart *parts, size_t part_count, int64_t timeout_millis);
OmqGoStatus omq_go_socket_send_one(OmqGoSocket *socket, const uint8_t *data, size_t len, int64_t timeout_millis);
OmqGoStatus omq_go_socket_try_send_batch(OmqGoSocket *socket, const OmqGoWireMessage *messages, size_t message_count, size_t *sent);
OmqGoStatus omq_go_receive_any(OmqGoSocket **sockets, size_t socket_count, int64_t timeout_millis, size_t *index, OmqGoMessage *out);
OmqGoStatus omq_go_socket_recv(OmqGoSocket *socket, int64_t timeout_millis, OmqGoMessage *out);
OmqGoStatus omq_go_socket_recv_one_into(OmqGoSocket *socket, int64_t timeout_millis, uint8_t *data, size_t capacity, size_t *written);
OmqGoStatus omq_go_socket_subscribe(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_unsubscribe(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_join(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_leave(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_wait_connected(OmqGoSocket *socket, size_t min_peers, int64_t timeout_millis, size_t *out);
OmqGoStatus omq_go_socket_wait_subscribed(OmqGoSocket *socket, uint64_t min_subscriptions, int64_t timeout_millis, uint64_t *out);
OmqGoStatus omq_go_socket_close(OmqGoSocket *socket, int64_t linger_millis);
void omq_go_socket_free(OmqGoSocket *socket);

OmqGoStatus omq_go_socket_set_send_hwm(OmqGoSocket *socket, uint32_t value);
OmqGoStatus omq_go_socket_set_recv_hwm(OmqGoSocket *socket, uint32_t value);
OmqGoStatus omq_go_socket_set_linger(OmqGoSocket *socket, int64_t millis);
OmqGoStatus omq_go_socket_set_identity(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_set_heartbeat_interval(OmqGoSocket *socket, int64_t millis);
OmqGoStatus omq_go_socket_set_handshake_timeout(OmqGoSocket *socket, int64_t millis);
OmqGoStatus omq_go_socket_set_max_message_size(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_plain_server(OmqGoSocket *socket, const char *username, const char *password);
OmqGoStatus omq_go_socket_set_plain_server_callback(OmqGoSocket *socket, uint64_t callback_id);
OmqGoStatus omq_go_socket_set_plain_client(OmqGoSocket *socket, const char *username, const char *password);
OmqGoStatus omq_go_socket_set_curve_server(OmqGoSocket *socket, const char *public_key, const char *secret_key);
OmqGoStatus omq_go_socket_set_curve_server_callback(OmqGoSocket *socket, const char *public_key, const char *secret_key, uint64_t callback_id);
OmqGoStatus omq_go_socket_set_curve_client(OmqGoSocket *socket, const char *public_key, const char *secret_key, const char *server_public_key);
OmqGoStatus omq_go_socket_set_workload_profile(OmqGoSocket *socket, int32_t profile);
OmqGoStatus omq_go_socket_set_reconnect(OmqGoSocket *socket, int32_t mode, int64_t min_millis, int64_t max_millis);
OmqGoStatus omq_go_socket_set_reconnect_stop_conn_refused(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_heartbeat_ttl(OmqGoSocket *socket, int64_t millis);
OmqGoStatus omq_go_socket_set_heartbeat_timeout(OmqGoSocket *socket, int64_t millis);
OmqGoStatus omq_go_socket_set_max_pending_handshakes(OmqGoSocket *socket, size_t max);
OmqGoStatus omq_go_socket_set_conflate(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_router_mandatory(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_on_mute(OmqGoSocket *socket, int32_t mode);
OmqGoStatus omq_go_socket_set_tcp_keepalive(OmqGoSocket *socket, int32_t mode, int64_t idle_millis, int64_t interval_millis, uint32_t count);
OmqGoStatus omq_go_socket_set_send_buffer_size(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_recv_buffer_size(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_xpub_nodrop(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_compression_auto_train(OmqGoSocket *socket, int enabled);
OmqGoStatus omq_go_socket_set_compression_threshold(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_compression_level(OmqGoSocket *socket, int64_t level);
OmqGoStatus omq_go_socket_set_compression_dict(OmqGoSocket *socket, const uint8_t *data, size_t len);
OmqGoStatus omq_go_socket_set_compression_dict_capacity(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_max_recv_dict_size(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_compression_offload_threshold(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_large_message_threshold(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_arena_threshold(OmqGoSocket *socket, int64_t bytes);
OmqGoStatus omq_go_socket_set_transmit_slot_cap(OmqGoSocket *socket, int64_t bytes);

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
OmqGoStatus omq_go_recv_ring_fill_cancelable(OmqGoRecvRing *ring, const OmqGoCancel *cancel, size_t max_messages);
void omq_go_recv_ring_close(OmqGoRecvRing *ring);

OmqGoCancel *omq_go_cancel_new(void);
void omq_go_cancel(const OmqGoCancel *cancel);
void omq_go_cancel_register_current(const OmqGoCancel *cancel);
void omq_go_cancel_free(OmqGoCancel *cancel);

void omq_go_message_free(OmqGoMessage message);
void omq_go_event_free(OmqGoEvent event);
void omq_go_string_free(char *value);

#ifdef __cplusplus
}
#endif

#endif
