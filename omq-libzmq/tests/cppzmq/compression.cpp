#include "common.hpp"

using namespace cppzmq_tests;

namespace {
void push_pull_compressed(const char *name,
                          std::string (*bind_fn)(zmq::socket_t &),
                          unsigned char byte)
{
    zmq::context_t ctx(1);
    zmq::socket_t push(ctx, zmq::socket_type::push);
    zmq::socket_t pull(ctx, zmq::socket_type::pull);
    set_timeouts(push);
    set_timeouts(pull);

    const std::string endpoint = bind_fn(pull);
    push.connect(endpoint);
    settle();

    const std::vector<unsigned char> payload(4096, byte);
    send_bytes(push, payload.data(), payload.size());

    zmq::message_t msg;
    const auto got = pull.recv(msg, zmq::recv_flags::none);
    expect(got && *got == payload.size(), name);
    expect(msg.size() == payload.size(), "compressed payload size mismatch");
    const auto *data = static_cast<const unsigned char *>(msg.data());
    expect(std::equal(data, data + msg.size(), payload.begin()),
           "compressed payload mismatch");
}

void pub_sub_compressed(std::string (*bind_fn)(zmq::socket_t &))
{
    zmq::context_t ctx(1);
    zmq::socket_t pub(ctx, zmq::socket_type::pub);
    zmq::socket_t sub(ctx, zmq::socket_type::sub);
    set_timeouts(pub);
    set_timeouts(sub);

    const std::string endpoint = bind_fn(pub);
    sub.set(zmq::sockopt::subscribe, "metrics");
    sub.connect(endpoint);
    settle();

    for (int i = 0; i < 5; ++i) {
        send_text(pub, "metrics:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    const std::string got = recv_string(sub);
    expect_payload(got, "metrics:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
}
} // namespace

int main()
{
    push_pull_compressed("lz4+tcp PUSH/PULL", bind_random_lz4_tcp, 0x42);
    push_pull_compressed("zstd+tcp PUSH/PULL", bind_random_zstd_tcp, 0x37);
    pub_sub_compressed(bind_random_lz4_tcp);
    pub_sub_compressed(bind_random_zstd_tcp);
    print_passed("compression");
    return 0;
}
