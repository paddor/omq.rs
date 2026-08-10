#include "common.hpp"

#include <type_traits>

using namespace cppzmq_tests;

namespace {
void timer_handler(int, void *arg)
{
    *static_cast<bool *>(arg) = true;
}

void verify_signatures()
{
    static_assert(
        std::is_same<decltype(&zmq_z85_encode),
                     char *(*)(char *, const uint8_t *, size_t)>::value,
        "zmq_z85_encode signature mismatch");
    static_assert(
        std::is_same<decltype(&zmq_sendmsg),
                     int (*)(void *, zmq_msg_t *, int)>::value,
        "zmq_sendmsg signature mismatch");
    static_assert(
        std::is_same<decltype(&zmq_recvmsg),
                     int (*)(void *, zmq_msg_t *, int)>::value,
        "zmq_recvmsg signature mismatch");
    static_assert(
        std::is_same<decltype(&zmq_sendiov),
                     int (*)(void *, struct iovec *, size_t, int)>::value,
        "zmq_sendiov signature mismatch");
    static_assert(
        std::is_same<decltype(&zmq_recviov),
                     int (*)(void *, struct iovec *, size_t *, int)>::value,
        "zmq_recviov signature mismatch");
    static_assert(
        std::is_same<decltype(&zmq_device),
                     int (*)(int, void *, void *)>::value,
        "zmq_device signature mismatch");
    static_assert(
        std::is_same<decltype(&zmq_threadstart),
                     void *(*)(zmq_thread_fn *, void *)>::value,
        "zmq_threadstart signature mismatch");
    static_assert(ZMQ_HAS_CAPABILITIES == 1,
                  "ZMQ_HAS_CAPABILITIES mismatch");
    static_assert(ZMQ_STREAMER == 1 && ZMQ_FORWARDER == 2 && ZMQ_QUEUE == 3,
                  "legacy device aliases mismatch");
    static_assert(sizeof(zmq_poller_event_t)
                      >= sizeof(void *) * 2 + sizeof(zmq_fd_t)
                             + sizeof(short),
                  "zmq_poller_event_t ABI too small");
}

void ctx_ext_roundtrip()
{
    void *ctx = zmq_ctx_new();
    expect(ctx != nullptr, "ctx new failed");

    int value = 2;
    int rc = zmq_ctx_set_ext(ctx, ZMQ_IO_THREADS, &value, sizeof(value));
    expect(rc == 0, "zmq_ctx_set_ext failed");

    int got = 0;
    size_t got_size = sizeof(got);
    rc = zmq_ctx_get_ext(ctx, ZMQ_IO_THREADS, &got, &got_size);
    expect(rc == 0, "zmq_ctx_get_ext failed");
    expect(got == value, "zmq_ctx_get_ext value mismatch");
    expect(got_size == sizeof(got), "zmq_ctx_get_ext size mismatch");

    zmq_ctx_term(ctx);
}

void timers_fire()
{
    void *timers = zmq_timers_new();
    expect(timers != nullptr, "zmq_timers_new failed");

    bool fired = false;
    const int id = zmq_timers_add(timers, 1, timer_handler, &fired);
    expect(id > 0, "zmq_timers_add failed");
    expect(zmq_timers_timeout(timers) >= 0, "zmq_timers_timeout failed");

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    expect(zmq_timers_execute(timers) == 0, "zmq_timers_execute failed");
    expect(fired, "timer did not fire");

    expect(zmq_timers_cancel(timers, id) == 0, "zmq_timers_cancel failed");
    expect(zmq_timers_timeout(timers) == -1, "timer was not canceled");

    expect(zmq_timers_destroy(&timers) == 0, "zmq_timers_destroy failed");
    expect(timers == nullptr, "zmq_timers_destroy did not null pointer");
}

void poller_socket_roundtrip()
{
    zmq::context_t ctx(1);
    zmq::socket_t push(ctx, zmq::socket_type::push);
    zmq::socket_t pull(ctx, zmq::socket_type::pull);
    set_timeouts(push);
    set_timeouts(pull);

    const std::string endpoint = bind_random_tcp(pull);
    push.connect(endpoint);
    settle();

    zmq::poller_t<> poller;
    poller.add(pull, zmq::event_flags::pollin);
    expect(poller.size() == 1, "poller size mismatch");

    std::vector<zmq::poller_event<>> events(1);
    expect(poller.wait_all(events, std::chrono::milliseconds(1)) == 0,
           "poller unexpectedly readable");

    send_text(push, "poller");
    expect(poller.wait_all(events, std::chrono::milliseconds(5000)) == 1,
           "poller did not report readable socket");
    expect(events[0].socket == pull, "poller socket mismatch");
    expect((events[0].events & zmq::event_flags::pollin)
               == zmq::event_flags::pollin,
           "poller event flags mismatch");
    expect_payload(recv_string(pull), "poller");
}
} // namespace

int main()
{
    verify_signatures();
    ctx_ext_roundtrip();
    timers_fire();
    poller_socket_roundtrip();
    print_passed("abi");
    return 0;
}
