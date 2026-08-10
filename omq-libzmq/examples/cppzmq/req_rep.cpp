#ifndef ZMQ_BUILD_DRAFT_API
#define ZMQ_BUILD_DRAFT_API
#endif

#include <zmq.hpp>

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

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

void send_text(zmq::socket_t &socket, const std::string &text)
{
    const auto sent = socket.send(zmq::buffer(text), zmq::send_flags::none);
    if (!sent || *sent != text.size()) {
        throw std::runtime_error("send failed");
    }
}
} // namespace

int main()
{
    try {
        zmq::context_t ctx(1);
        const char *endpoint = "inproc://cppzmq-example-req-rep";

        std::thread server([&] {
            zmq::socket_t rep(ctx, zmq::socket_type::rep);
            rep.set(zmq::sockopt::linger, 0);
            rep.set(zmq::sockopt::rcvtimeo, 1000);
            rep.set(zmq::sockopt::sndtimeo, 1000);
            rep.bind(endpoint);

            for (int i = 0; i < 3; ++i) {
                const std::string request = recv_text(rep);
                send_text(rep, "echo:" + request);
            }
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        zmq::socket_t req(ctx, zmq::socket_type::req);
        req.set(zmq::sockopt::linger, 0);
        req.set(zmq::sockopt::rcvtimeo, 1000);
        req.set(zmq::sockopt::sndtimeo, 1000);
        req.connect(endpoint);

        for (int i = 0; i < 3; ++i) {
            const std::string request = "hello-" + std::to_string(i);
            send_text(req, request);
            std::cout << "client: " << request << " -> " << recv_text(req)
                      << '\n';
        }

        server.join();
        std::cout << "cppzmq req_rep passed\n";
    } catch (const zmq::error_t &e) {
        std::cerr << "zmq error: " << e.what() << '\n';
        return 1;
    } catch (const std::exception &e) {
        std::cerr << "error: " << e.what() << '\n';
        return 1;
    }
}
