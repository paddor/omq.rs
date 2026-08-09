#include "common.hpp"

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#endif

using namespace cppzmq_tests;

namespace {
#ifndef _WIN32
class RawTcpSocket
{
  public:
    explicit RawTcpSocket(int fd) : fd_(fd) {}
    RawTcpSocket(const RawTcpSocket &) = delete;
    RawTcpSocket &operator=(const RawTcpSocket &) = delete;
    ~RawTcpSocket()
    {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    void write_all(const char *data, size_t len)
    {
        size_t written = 0;
        while (written < len) {
            const ssize_t n = ::send(fd_, data + written, len - written, 0);
            expect(n > 0, "raw TCP send failed");
            written += static_cast<size_t>(n);
        }
    }

    std::string read_some()
    {
        char buf[128];
        const ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
        expect(n > 0, "raw TCP recv failed");
        return {buf, static_cast<size_t>(n)};
    }

  private:
    int fd_;
};

uint16_t port_from_endpoint(const std::string &endpoint)
{
    const size_t pos = endpoint.rfind(':');
    expect(pos != std::string::npos, "bad TCP endpoint");
    const int port = std::stoi(endpoint.substr(pos + 1));
    expect(port > 0 && port <= 65535, "bad TCP port");
    return static_cast<uint16_t>(port);
}

RawTcpSocket connect_raw_tcp(const std::string &endpoint)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    expect(fd >= 0, "raw TCP socket failed");

    timeval tv {};
    tv.tv_sec = 5;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_from_endpoint(endpoint));
    const int parsed = ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    expect(parsed == 1, "inet_pton failed");

    const int rc =
        ::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    expect(rc == 0, "raw TCP connect failed");
    return RawTcpSocket(fd);
}
#endif

void verify_abi()
{
    int major = 0;
    int minor = 0;
    int patch = 0;
    zmq::version(&major, &minor, &patch);
    expect(major == 4 && minor == 3 && patch == 6,
           "runtime zmq_version mismatch");

    zmq::context_t ctx(1);
    expect(ctx.get(zmq::ctxopt::msg_t_size) == 64, "ZMQ_MSG_T_SIZE mismatch");
}

void pair_inproc()
{
    zmq::context_t ctx(1);
    zmq::socket_t a(ctx, zmq::socket_type::pair);
    zmq::socket_t b(ctx, zmq::socket_type::pair);
    expect_socket_type(a, zmq::socket_type::pair);
    expect_socket_type(b, zmq::socket_type::pair);
    set_timeouts(a);
    set_timeouts(b);

    a.bind("inproc://cppzmq-pair");
    b.connect("inproc://cppzmq-pair");
    settle();

    send_text(a, "pair-a");
    send_text(b, "pair-b");
    expect_payload(recv_string(b), "pair-a");
    expect_payload(recv_string(a), "pair-b");
}

void push_pull_tcp()
{
    zmq::context_t ctx(1);
    zmq::socket_t push(ctx, zmq::socket_type::push);
    zmq::socket_t pull(ctx, zmq::socket_type::pull);
    expect_socket_type(push, zmq::socket_type::push);
    expect_socket_type(pull, zmq::socket_type::pull);
    set_timeouts(push);
    set_timeouts(pull);

    const std::string endpoint = bind_random_tcp(pull);
    push.connect(endpoint);
    settle();

    send_text(push, "push-pull");
    expect_payload(recv_string(pull), "push-pull");
}

void pub_sub_tcp()
{
    zmq::context_t ctx(1);
    zmq::socket_t pub(ctx, zmq::socket_type::pub);
    zmq::socket_t sub(ctx, zmq::socket_type::sub);
    expect_socket_type(pub, zmq::socket_type::pub);
    expect_socket_type(sub, zmq::socket_type::sub);
    set_timeouts(pub);
    set_timeouts(sub);

    const std::string endpoint = bind_random_tcp(pub);
    sub.set(zmq::sockopt::subscribe, "topic");
    sub.connect(endpoint);
    settle();

    for (int i = 0; i < 5; ++i) {
        send_text(pub, "topic-data");
    }
    expect_payload(recv_string(sub), "topic-data");
}

void req_rep_tcp()
{
    zmq::context_t ctx(1);
    zmq::socket_t req(ctx, zmq::socket_type::req);
    zmq::socket_t rep(ctx, zmq::socket_type::rep);
    expect_socket_type(req, zmq::socket_type::req);
    expect_socket_type(rep, zmq::socket_type::rep);
    set_timeouts(req);
    set_timeouts(rep);

    const std::string endpoint = bind_random_tcp(rep);
    req.connect(endpoint);
    settle();

    send_text(req, "ping");
    expect_payload(recv_string(rep), "ping");
    send_text(rep, "pong");
    expect_payload(recv_string(req), "pong");
}

void dealer_router_tcp()
{
    zmq::context_t ctx(1);
    zmq::socket_t dealer(ctx, zmq::socket_type::dealer);
    zmq::socket_t router(ctx, zmq::socket_type::router);
    expect_socket_type(dealer, zmq::socket_type::dealer);
    expect_socket_type(router, zmq::socket_type::router);
    set_timeouts(dealer);
    set_timeouts(router);

    dealer.set(zmq::sockopt::routing_id, "dealer-a");
    const std::string endpoint = bind_random_tcp(router);
    dealer.connect(endpoint);
    settle();

    send_text(dealer, "hello");
    const std::vector<std::string> request = recv_strings(router);
    expect_size(request.size(), 2);
    expect_payload(request[0], "dealer-a");
    expect_payload(request[1], "hello");

    send_two(router, request[0], "world");
    expect_payload(recv_string(dealer), "world");
}

void xpub_xsub_inproc()
{
    zmq::context_t ctx(1);
    zmq::socket_t xpub(ctx, zmq::socket_type::xpub);
    zmq::socket_t xsub(ctx, zmq::socket_type::xsub);
    expect_socket_type(xpub, zmq::socket_type::xpub);
    expect_socket_type(xsub, zmq::socket_type::xsub);
    set_timeouts(xpub);
    set_timeouts(xsub);

    xpub.bind("inproc://cppzmq-xpub-xsub");
    xsub.connect("inproc://cppzmq-xpub-xsub");
    settle();

    const std::string subscription("\001topic", 6);
    send_string(xsub, subscription);
    const std::string got_subscription = recv_string(xpub);
    expect(got_subscription == subscription, "XPUB subscription mismatch");

    send_text(xpub, "topic-body");
    expect_payload(recv_string(xsub), "topic-body");
}

void stream_tcp()
{
#ifndef _WIN32
    zmq::context_t ctx(1);
    zmq::socket_t stream(ctx, zmq::socket_type::stream);
    expect_socket_type(stream, zmq::socket_type::stream);
    set_timeouts(stream);

    const std::string endpoint = bind_random_tcp(stream);
    RawTcpSocket raw = connect_raw_tcp(endpoint);

    std::vector<std::string> frames = recv_strings(stream);
    expect_size(frames.size(), 2);
    expect(!frames[0].empty(), "STREAM identity is empty");
    expect(frames[1].empty(), "STREAM connect notification data not empty");
    const std::string identity = frames[0];

    raw.write_all("stream-in", 9);
    frames = recv_strings(stream);
    expect_size(frames.size(), 2);
    expect(frames[0] == identity, "STREAM identity mismatch");
    expect_payload(frames[1], "stream-in");

    send_two(stream, identity, "stream-out");
    expect_payload(raw.read_some(), "stream-out");
#endif
}

void server_client_tcp()
{
    zmq::context_t ctx(1);
    zmq::socket_t server(ctx, zmq::socket_type::server);
    zmq::socket_t client(ctx, zmq::socket_type::client);
    expect_socket_type(server, zmq::socket_type::server);
    expect_socket_type(client, zmq::socket_type::client);
    set_timeouts(server);
    set_timeouts(client);

    const std::string endpoint = bind_random_tcp(server);
    client.connect(endpoint);
    settle();

    send_text(client, "request");
    const std::vector<std::string> frames = recv_strings(server);
    expect_size(frames.size(), 2);
    expect(!frames[0].empty(), "SERVER routing id missing");
    expect_payload(frames[1], "request");

    send_two(server, frames[0], "reply");
    expect_payload(recv_string(client), "reply");
}

void radio_dish_inproc()
{
    zmq::context_t ctx(1);
    zmq::socket_t radio(ctx, zmq::socket_type::radio);
    zmq::socket_t dish(ctx, zmq::socket_type::dish);
    expect_socket_type(radio, zmq::socket_type::radio);
    expect_socket_type(dish, zmq::socket_type::dish);
    set_timeouts(radio);
    set_timeouts(dish);

    radio.bind("inproc://cppzmq-radio-dish");
    dish.join("weather");
    dish.connect("inproc://cppzmq-radio-dish");
    settle();

    zmq::message_t msg("weather sunny", 13);
    msg.set_group("weather");
    const auto sent = radio.send(msg, zmq::send_flags::none);
    expect(sent && *sent == 13, "RADIO send failed");

    expect_payload(recv_string(dish), "weather sunny");
}

void scatter_gather_tcp()
{
    zmq::context_t ctx(1);
    zmq::socket_t scatter(ctx, zmq::socket_type::scatter);
    zmq::socket_t gather(ctx, zmq::socket_type::gather);
    expect_socket_type(scatter, zmq::socket_type::scatter);
    expect_socket_type(gather, zmq::socket_type::gather);
    set_timeouts(scatter);
    set_timeouts(gather);

    const std::string endpoint = bind_random_tcp(gather);
    scatter.connect(endpoint);
    settle();

    send_text(scatter, "scatter");
    expect_payload(recv_string(gather), "scatter");
}

void peer_inproc()
{
    zmq::context_t ctx(1);
    zmq::socket_t a(ctx, zmq::socket_type::peer);
    zmq::socket_t b(ctx, zmq::socket_type::peer);
    expect_socket_type(a, zmq::socket_type::peer);
    expect_socket_type(b, zmq::socket_type::peer);
    set_timeouts(a);
    set_timeouts(b);

    a.set(zmq::sockopt::routing_id, "peer-a");
    b.set(zmq::sockopt::routing_id, "peer-b");
    a.bind("inproc://cppzmq-peer");
    b.connect("inproc://cppzmq-peer");
    settle();

    send_two(b, "peer-a", "hello-a");
    std::vector<std::string> frames = recv_strings(a);
    expect_size(frames.size(), 2);
    expect_payload(frames[0], "peer-b");
    expect_payload(frames[1], "hello-a");

    send_two(a, "peer-b", "hello-b");
    frames = recv_strings(b);
    expect_size(frames.size(), 2);
    expect_payload(frames[0], "peer-a");
    expect_payload(frames[1], "hello-b");
}

void channel_inproc()
{
    zmq::context_t ctx(1);
    zmq::socket_t a(ctx, zmq::socket_type::channel);
    zmq::socket_t b(ctx, zmq::socket_type::channel);
    expect_socket_type(a, zmq::socket_type::channel);
    expect_socket_type(b, zmq::socket_type::channel);
    set_timeouts(a);
    set_timeouts(b);

    a.bind("inproc://cppzmq-channel");
    b.connect("inproc://cppzmq-channel");
    settle();

    send_text(a, "chan-a");
    send_text(b, "chan-b");
    expect_payload(recv_string(b), "chan-a");
    expect_payload(recv_string(a), "chan-b");
}

void dgram_rejected()
{
    zmq::context_t ctx(1);
    bool rejected = false;
    try {
        zmq::socket_t dgram(ctx, zmq::socket_type::dgram);
    } catch (const zmq::error_t &err) {
        rejected = err.num() == EINVAL;
    }
    expect(rejected, "DGRAM should be rejected");
}
} // namespace

int main()
{
    verify_abi();
    pair_inproc();
    push_pull_tcp();
    pub_sub_tcp();
    req_rep_tcp();
    dealer_router_tcp();
    xpub_xsub_inproc();
    stream_tcp();
    server_client_tcp();
    radio_dish_inproc();
    scatter_gather_tcp();
    peer_inproc();
    channel_inproc();
    dgram_rejected();
    print_passed("socket types");
    return 0;
}
