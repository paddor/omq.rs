/*  libzmq-compatible header for omq-libzmq.                                 */
/*  Matches libzmq 4.3.6 signatures and macro constants.                      */
/*  Drop-in: #include <zmq.h> and link -lomq_zmq (or rename to -lomq).       */

#ifndef __ZMQ_H_INCLUDED__
#define __ZMQ_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

#include <errno.h>
#include <stddef.h>
#include <stdint.h>

/*  Version ------------------------------------------------------------------ */
#define ZMQ_VERSION_MAJOR 4
#define ZMQ_VERSION_MINOR 3
#define ZMQ_VERSION_PATCH 6
#define ZMQ_MAKE_VERSION(major, minor, patch) \
    ((major) * 10000 + (minor) * 100 + (patch))
#define ZMQ_VERSION \
    ZMQ_MAKE_VERSION (ZMQ_VERSION_MAJOR, ZMQ_VERSION_MINOR, ZMQ_VERSION_PATCH)

/*  Error codes -------------------------------------------------------------- */
/*  ZMQ-specific errno base (HAUSNUMERO).                                     */
#define ZMQ_HAUSNUMERO 156384712

/*  These may already be defined by the platform; only define if missing.    */
#ifndef ENOTSUP
#define ENOTSUP         (ZMQ_HAUSNUMERO + 1)
#endif
#ifndef EPROTONOSUPPORT
#define EPROTONOSUPPORT (ZMQ_HAUSNUMERO + 2)
#endif
#ifndef ENOBUFS
#define ENOBUFS         (ZMQ_HAUSNUMERO + 3)
#endif
#ifndef ENETDOWN
#define ENETDOWN        (ZMQ_HAUSNUMERO + 4)
#endif
#ifndef EADDRINUSE
#define EADDRINUSE      (ZMQ_HAUSNUMERO + 5)
#endif
#ifndef EADDRNOTAVAIL
#define EADDRNOTAVAIL   (ZMQ_HAUSNUMERO + 6)
#endif
#ifndef ECONNREFUSED
#define ECONNREFUSED    (ZMQ_HAUSNUMERO + 7)
#endif
#ifndef EINPROGRESS
#define EINPROGRESS     (ZMQ_HAUSNUMERO + 8)
#endif
#ifndef ENOTSOCK
#define ENOTSOCK        (ZMQ_HAUSNUMERO + 9)
#endif
#ifndef EMSGSIZE
#define EMSGSIZE        (ZMQ_HAUSNUMERO + 10)
#endif

/*  ZMQ-specific errors always defined with their OMQ values.                */
#define EFSM            (ZMQ_HAUSNUMERO + 51)
#define ENOCOMPATPROTO  (ZMQ_HAUSNUMERO + 52)
#define ETERM           (ZMQ_HAUSNUMERO + 53)
#define EMTHREAD        (ZMQ_HAUSNUMERO + 54)

/*  Context options ---------------------------------------------------------- */
#define ZMQ_IO_THREADS          1
#define ZMQ_MAX_SOCKETS         2
#define ZMQ_SOCKET_LIMIT        3
#define ZMQ_THREAD_PRIORITY     3
#define ZMQ_THREAD_SCHED_POLICY 4
#define ZMQ_MAX_MSGSZ           5
#define ZMQ_MSG_T_SIZE          6
#define ZMQ_THREAD_AFFINITY_CPU_ADD    7
#define ZMQ_THREAD_AFFINITY_CPU_REMOVE 8
#define ZMQ_THREAD_NAME_PREFIX  9
#define ZMQ_ZERO_COPY_RECV      10

/*  Default value for ZMQ_MAX_SOCKETS.                                       */
#define ZMQ_MAX_SOCKETS_DFLT    1023
/*  Default value for ZMQ_IO_THREADS.                                        */
#define ZMQ_IO_THREADS_DFLT     1

/*  Socket types ------------------------------------------------------------- */
#define ZMQ_PAIR        0
#define ZMQ_PUB         1
#define ZMQ_SUB         2
#define ZMQ_REQ         3
#define ZMQ_REP         4
#define ZMQ_DEALER      5
#define ZMQ_ROUTER      6
#define ZMQ_PULL        7
#define ZMQ_PUSH        8
#define ZMQ_XPUB        9
#define ZMQ_XSUB        10
#define ZMQ_STREAM      11
#define ZMQ_SERVER      12
#define ZMQ_CLIENT      13
#define ZMQ_RADIO       14
#define ZMQ_DISH        15
#define ZMQ_GATHER      16
#define ZMQ_SCATTER     17
#define ZMQ_DGRAM       18
#define ZMQ_PEER        19
#define ZMQ_CHANNEL     20

/*  Socket options ----------------------------------------------------------- */
#define ZMQ_AFFINITY                4
#define ZMQ_ROUTING_ID              5
#define ZMQ_IDENTITY                5   /* deprecated alias */
#define ZMQ_SUBSCRIBE               6
#define ZMQ_UNSUBSCRIBE             7
#define ZMQ_RATE                    8
#define ZMQ_RECOVERY_IVL            9
#define ZMQ_SNDBUF                  11
#define ZMQ_RCVBUF                  12
#define ZMQ_RCVMORE                 13
#define ZMQ_FD                      14
#define ZMQ_EVENTS                  15
#define ZMQ_TYPE                    16
#define ZMQ_LINGER                  17
#define ZMQ_RECONNECT_IVL           18
#define ZMQ_BACKLOG                 19
#define ZMQ_RECONNECT_IVL_MAX       21
#define ZMQ_MAXMSGSIZE              22
#define ZMQ_SNDHWM                  23
#define ZMQ_RCVHWM                  24
#define ZMQ_MULTICAST_HOPS          25
#define ZMQ_RCVTIMEO                27
#define ZMQ_SNDTIMEO                28
#define ZMQ_LAST_ENDPOINT           32
#define ZMQ_ROUTER_MANDATORY        33
#define ZMQ_TCP_KEEPALIVE           34
#define ZMQ_TCP_KEEPALIVE_CNT       35
#define ZMQ_TCP_KEEPALIVE_IDLE      36
#define ZMQ_TCP_KEEPALIVE_INTVL     37
#define ZMQ_IMMEDIATE               39
#define ZMQ_XPUB_VERBOSE            40
#define ZMQ_ROUTER_RAW              41
#define ZMQ_IPV6                    42
#define ZMQ_MECHANISM               43
#define ZMQ_PLAIN_SERVER            44
#define ZMQ_PLAIN_USERNAME          45
#define ZMQ_PLAIN_PASSWORD          46
#define ZMQ_CURVE_SERVER            47
#define ZMQ_CURVE_PUBLICKEY         48
#define ZMQ_CURVE_SECRETKEY         49
#define ZMQ_CURVE_SERVERKEY         50
#define ZMQ_PROBE_ROUTER            51
#define ZMQ_REQ_CORRELATE           52
#define ZMQ_REQ_RELAXED             53
#define ZMQ_CONFLATE                54
#define ZMQ_ZAP_DOMAIN              55
#define ZMQ_ROUTER_HANDOVER         56
#define ZMQ_TOS                     57
#define ZMQ_CONNECT_ROUTING_ID      61
#define ZMQ_GSSAPI_SERVER           62
#define ZMQ_GSSAPI_PRINCIPAL        63
#define ZMQ_GSSAPI_SERVICE_PRINCIPAL 64
#define ZMQ_GSSAPI_PLAINTEXT        65
#define ZMQ_HANDSHAKE_IVL           66
#define ZMQ_SOCKS_PROXY             68
#define ZMQ_XPUB_NODROP             69
#define ZMQ_BLOCKY                  70
#define ZMQ_XPUB_MANUAL             71
#define ZMQ_XPUB_WELCOME_MSG        72
#define ZMQ_STREAM_NOTIFY           73
#define ZMQ_INVERT_MATCHING         74
#define ZMQ_HEARTBEAT_IVL           75
#define ZMQ_HEARTBEAT_TTL           76
#define ZMQ_HEARTBEAT_TIMEOUT       77
#define ZMQ_XPUB_VERBOSER           78
#define ZMQ_CONNECT_TIMEOUT         79
#define ZMQ_TCP_MAXRT               80
#define ZMQ_THREAD_SAFE             81
#define ZMQ_MULTICAST_MAXTPDU       84
#define ZMQ_VMCI_BUFFER_SIZE        85
#define ZMQ_VMCI_BUFFER_MIN_SIZE    86
#define ZMQ_VMCI_BUFFER_MAX_SIZE    87
#define ZMQ_VMCI_CONNECT_TIMEOUT    88
#define ZMQ_USE_FD                  89
#define ZMQ_GSSAPI_PRINCIPAL_NAMETYPE          90
#define ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE  91
#define ZMQ_BINDTODEVICE            92
#define ZMQ_MULTICAST_LOOP          96
#define ZMQ_ROUTER_NOTIFY           97

/*  omq-libzmq send/HWM option notes ---------------------------------------- */
/*
    ZMQ_SNDHWM and ZMQ_RCVHWM are message-count limits, not byte limits.
    A complete multipart message consumes one HWM slot regardless of payload
    byte size.

    ZMQ_LINGER defaults to -1, matching libzmq. zmq_close() returns quickly;
    context termination waits for active linger work.

    ZMQ_IMMEDIATE defaults to 0. Connected sockets may queue before a peer is
    ready. With ZMQ_IMMEDIATE=1, connected no-peer round-robin sends mute
    until handshake completes.

    Bound PUSH/DEALER/REQ/CLIENT/SCATTER sockets with no ready peer are muted:
    blocking zmq_send() waits for a route, while ZMQ_DONTWAIT or SNDTIMEO=0
    returns EAGAIN. omq-libzmq does not accept and drop these messages.
*/

/*  Message options ---------------------------------------------------------- */
#define ZMQ_MORE                    1
#define ZMQ_SHARED                  3

/*  Send/recv flags ---------------------------------------------------------- */
#define ZMQ_DONTWAIT    1
#define ZMQ_SNDMORE     2
#define ZMQ_NOBLOCK     ZMQ_DONTWAIT    /* deprecated alias */

/*  Security mechanism ------------------------------------------------------- */
#define ZMQ_NULL    0
#define ZMQ_PLAIN   1
#define ZMQ_CURVE   2
#define ZMQ_GSSAPI  3

/*  RADIO-DISH socket API ---------------------------------------------------- */
#define ZMQ_GROUP_MAX_LENGTH 255

/*  Socket events (monitor) -------------------------------------------------- */
#define ZMQ_EVENT_CONNECTED              0x0001
#define ZMQ_EVENT_CONNECT_DELAYED        0x0002
#define ZMQ_EVENT_CONNECT_RETRIED        0x0004
#define ZMQ_EVENT_LISTENING              0x0008
#define ZMQ_EVENT_BIND_FAILED            0x0010
#define ZMQ_EVENT_ACCEPTED               0x0020
#define ZMQ_EVENT_ACCEPT_FAILED          0x0040
#define ZMQ_EVENT_CLOSED                 0x0080
#define ZMQ_EVENT_CLOSE_FAILED           0x0100
#define ZMQ_EVENT_DISCONNECTED           0x0200
#define ZMQ_EVENT_MONITOR_STOPPED        0x0400
#define ZMQ_EVENT_HANDSHAKE_FAILED_NO_DETAIL 0x0800
#define ZMQ_EVENT_HANDSHAKE_SUCCEEDED    0x1000
#define ZMQ_EVENT_HANDSHAKE_FAILED_PROTOCOL 0x2000
#define ZMQ_EVENT_HANDSHAKE_FAILED_AUTH  0x4000
#define ZMQ_EVENT_ALL                    0xFFFF

/*  Poll events -------------------------------------------------------------- */
#define ZMQ_POLLIN   1
#define ZMQ_POLLOUT  2
#define ZMQ_POLLERR  4
#define ZMQ_POLLPRI  8

/*  Types -------------------------------------------------------------------- */

/*  Handle to a message. Opaque 64-byte struct matching the layout used      */
/*  by omq-libzmq internally.                                                */
typedef struct zmq_msg_t
{
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_ARM64))
    __declspec(align(8)) unsigned char _[64];
#elif defined(_MSC_VER) && \
    (defined(_M_IX86) || defined(_M_ARM_ARMV7VE) || defined(_M_ARM))
    __declspec(align(4)) unsigned char _[64];
#elif defined(__GNUC__) || defined(__INTEL_COMPILER) || \
    (defined(__SUNPRO_C) && __SUNPRO_C >= 0x590) || \
    (defined(__SUNPRO_CC) && __SUNPRO_CC >= 0x590)
    unsigned char _[64] __attribute__((aligned(sizeof(void *))));
#else
    unsigned char _[64];
#endif
} zmq_msg_t;

/*  Free function for zmq_msg_init_data.                                     */
typedef void (zmq_free_fn) (void *data_, void *hint_);

/*  Poll item.                                                               */
typedef struct
{
    void *socket;
    int fd;
    short events;
    short revents;
} zmq_pollitem_t;

/*  The integer type of timeval members.                                     */
typedef long zmq_timeval_t;

/*  Context API -------------------------------------------------------------- */
void *zmq_ctx_new (void);
int   zmq_ctx_term (void *context_);
int   zmq_ctx_shutdown (void *context_);
int   zmq_ctx_set (void *context_, int option_, int optval_);
int   zmq_ctx_get (void *context_, int option_);
int   zmq_ctx_destroy (void *context_); /* deprecated */
void *zmq_init (int io_threads_);       /* deprecated */
int   zmq_term (void *context_);        /* deprecated */

/*  OMQ extension: share/import one native ContextCore inside this process.  */
int   omq_ctx_share_key (void *context_, uint64_t *key_hi_, uint64_t *key_lo_);
void *omq_ctx_from_share_key (uint64_t key_hi_, uint64_t key_lo_);

/*  Socket API --------------------------------------------------------------- */
void *zmq_socket     (void *context_, int type_);
int   zmq_close      (void *s_);
int   zmq_setsockopt (void *s_, int option_, const void *optval_,
                      size_t optvallen_);
int   zmq_getsockopt (void *s_, int option_, void *optval_,
                      size_t *optvallen_);
int   zmq_bind       (void *s_, const char *addr_);
int   zmq_connect    (void *s_, const char *addr_);
int   zmq_unbind     (void *s_, const char *addr_);
int   zmq_disconnect (void *s_, const char *addr_);

/*  Draft socket group membership (RADIO/DISH). */
int   zmq_join       (void *s_, const char *group_);
int   zmq_leave      (void *s_, const char *group_);

/*  Send/recv API ------------------------------------------------------------ */
int zmq_send       (void *s_, const void *buf_, size_t len_, int flags_);
int zmq_send_const (void *s_, const void *buf_, size_t len_, int flags_);
int zmq_recv       (void *s_, void *buf_, size_t len_, int flags_);

/*  Message API -------------------------------------------------------------- */
int   zmq_msg_init         (zmq_msg_t *msg_);
int   zmq_msg_init_size    (zmq_msg_t *msg_, size_t size_);
int   zmq_msg_init_buffer  (zmq_msg_t *msg_, const void *buf_, size_t size_);
int   zmq_msg_init_data    (zmq_msg_t *msg_, void *data_, size_t size_,
                             zmq_free_fn *ffn_, void *hint_);
int   zmq_msg_send         (zmq_msg_t *msg_, void *s_, int flags_);
int   zmq_msg_recv         (zmq_msg_t *msg_, void *s_, int flags_);

/*  Deprecated aliases (libzmq 3.x). */
int   zmq_sendmsg           (zmq_msg_t *msg_, void *s_, int flags_);
int   zmq_recvmsg           (zmq_msg_t *msg_, void *s_, int flags_);
int   zmq_msg_close        (zmq_msg_t *msg_);
int   zmq_msg_move         (zmq_msg_t *dest_, zmq_msg_t *src_);
int   zmq_msg_copy         (zmq_msg_t *dest_, zmq_msg_t *src_);
void *zmq_msg_data         (zmq_msg_t *msg_);
size_t zmq_msg_size        (const zmq_msg_t *msg_);
int   zmq_msg_more         (const zmq_msg_t *msg_);
int   zmq_msg_get          (const zmq_msg_t *msg_, int property_);
int   zmq_msg_set          (zmq_msg_t *msg_, int property_, int optval_);
const char *zmq_msg_gets   (const zmq_msg_t *msg_, const char *property_);
uint32_t zmq_msg_routing_id      (zmq_msg_t *msg_);
int      zmq_msg_set_routing_id  (zmq_msg_t *msg_, uint32_t routing_id_);
const char *zmq_msg_group        (zmq_msg_t *msg_);
int         zmq_msg_set_group    (zmq_msg_t *msg_, const char *group_);

/*  Poll API ----------------------------------------------------------------- */
int zmq_poll (zmq_pollitem_t *items_, int nitems_, long timeout_);

/*  Proxy API ---------------------------------------------------------------- */
int zmq_proxy           (void *frontend_, void *backend_, void *capture_);
int zmq_proxy_steerable (void *frontend_, void *backend_, void *capture_,
                         void *control_);

/*  Monitor API -------------------------------------------------------------- */
int zmq_socket_monitor (void *s_, const char *addr_, int events_);

/*  Curve / Z85 API ---------------------------------------------------------- */
int zmq_curve_keypair (char *z85_public_key_, char *z85_secret_key_);
int zmq_curve_public  (char *z85_public_key_, const char *z85_secret_key_);
int zmq_z85_encode    (char *dest_, const uint8_t *data_, size_t size_);
uint8_t *zmq_z85_decode (uint8_t *dest_, const char *string_);

/*  Atomic counter API ------------------------------------------------------- */
void *zmq_atomic_counter_new     (void);
void  zmq_atomic_counter_set     (void *counter_, int value_);
int   zmq_atomic_counter_inc     (void *counter_);
int   zmq_atomic_counter_dec     (void *counter_);
int   zmq_atomic_counter_value   (void *counter_);
void  zmq_atomic_counter_destroy (void **counter_p_);

/*  Misc --------------------------------------------------------------------- */
int  zmq_errno  (void);
const char *zmq_strerror (int errnum_);
void zmq_version (int *major_, int *minor_, int *patch_);
int  zmq_has    (const char *capability_);
void zmq_sleep  (int seconds_);

/*  Deprecated: stopwatch, device, threadstart --------------------------------*/
void *zmq_stopwatch_start  (void);
unsigned long zmq_stopwatch_intermediate (void *watch_);
unsigned long zmq_stopwatch_stop (void *watch_);
int   zmq_device           (int type_, void *frontend_, void *backend_);
void *zmq_threadstart      (void (*func_) (void *), void *arg_);
void  zmq_threadclose      (void *thread_);

#ifdef __cplusplus
}
#endif

#endif /* __ZMQ_H_INCLUDED__ */
