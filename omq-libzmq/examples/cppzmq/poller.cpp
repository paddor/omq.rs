#ifndef ZMQ_BUILD_DRAFT_API
#define ZMQ_BUILD_DRAFT_API
#endif

#include <zmq.hpp>

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {
std::string recv_text(zmq::socket_t &socket)
{
    zmq::message_t msg;
    const auto got = socket.recv(msg, zmq::recv_flags::none);
    if (!got) {
        throw std::runtime_error("recv timed out");
    }
    return {static_cast<const char *>(msg.data()), msg.size()};
}
} // namespace

int main()
{
    try {
        zmq::context_t ctx(1);
        zmq::socket_t push(ctx, zmq::socket_type::push);
        zmq::socket_t pull(ctx, zmq::socket_type::pull);
        push.set(zmq::sockopt::linger, 0);
        pull.set(zmq::sockopt::linger, 0);
        pull.set(zmq::sockopt::rcvtimeo, 1000);

        pull.bind("tcp://127.0.0.1:*");
        const std::string endpoint = pull.get(zmq::sockopt::last_endpoint);
        push.connect(endpoint);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        zmq::poller_t<> poller;
        poller.add(pull, zmq::event_flags::pollin);

        const std::string payload = "poller";
        push.send(zmq::buffer(payload), zmq::send_flags::none);

        std::vector<zmq::poller_event<>> events(1);
        const auto n = poller.wait_all(events, std::chrono::seconds(1));
        if (n != 1 || events[0].socket != pull) {
            throw std::runtime_error("poller did not report pull socket");
        }

        std::cout << "poller: " << recv_text(pull) << '\n';
        std::cout << "cppzmq poller passed\n";
    } catch (const zmq::error_t &e) {
        std::cerr << "zmq error: " << e.what() << '\n';
        return 1;
    } catch (const std::exception &e) {
        std::cerr << "error: " << e.what() << '\n';
        return 1;
    }
}
