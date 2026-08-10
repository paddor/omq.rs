#pragma once

#ifndef ZMQ_BUILD_DRAFT_API
#define ZMQ_BUILD_DRAFT_API
#endif

#include "zmq.h"
#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static_assert(ZMQ_VERSION_MAJOR == 4, "expected omq-libzmq zmq.h");
static_assert(ZMQ_VERSION_MINOR == 3, "expected omq-libzmq zmq.h");
static_assert(ZMQ_VERSION_PATCH == 6, "expected omq-libzmq zmq.h");
static_assert(sizeof(zmq_msg_t) == 64, "zmq_msg_t ABI size mismatch");

namespace cppzmq_tests {
inline void expect(bool ok, const char *msg)
{
    if (!ok) {
        throw std::runtime_error(msg);
    }
}

inline std::string message_string(const zmq::message_t &msg)
{
    return {static_cast<const char *>(msg.data()), msg.size()};
}

inline void set_timeouts(zmq::socket_t &socket, int ms = 5000)
{
    socket.set(zmq::sockopt::linger, 0);
    socket.set(zmq::sockopt::sndtimeo, ms);
    socket.set(zmq::sockopt::rcvtimeo, ms);
}

inline void settle()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
}

inline std::string bind_random(zmq::socket_t &socket, const std::string &scheme)
{
    socket.bind(scheme + "://127.0.0.1:*");
    return socket.get(zmq::sockopt::last_endpoint);
}

inline std::string bind_random_tcp(zmq::socket_t &socket)
{
    return bind_random(socket, "tcp");
}

inline std::string bind_random_lz4_tcp(zmq::socket_t &socket)
{
    return bind_random(socket, "lz4+tcp");
}

inline std::string bind_random_zstd_tcp(zmq::socket_t &socket)
{
    return bind_random(socket, "zstd+tcp");
}

inline void send_bytes(zmq::socket_t &socket,
                       const void *data,
                       size_t size,
                       zmq::send_flags flags = zmq::send_flags::none)
{
    const auto sent = socket.send(zmq::buffer(data, size), flags);
    expect(sent && *sent == size, "send failed");
}

inline void send_string(zmq::socket_t &socket,
                        const std::string &data,
                        zmq::send_flags flags = zmq::send_flags::none)
{
    send_bytes(socket, data.data(), data.size(), flags);
}

inline void send_text(zmq::socket_t &socket,
                      const char *text,
                      zmq::send_flags flags = zmq::send_flags::none)
{
    send_bytes(socket, text, std::strlen(text), flags);
}

inline void send_two(zmq::socket_t &socket,
                     const std::string &first,
                     const std::string &second)
{
    send_string(socket, first, zmq::send_flags::sndmore);
    send_string(socket, second);
}

inline std::string recv_string(zmq::socket_t &socket)
{
    zmq::message_t msg;
    const auto got = socket.recv(msg, zmq::recv_flags::none);
    expect(got.has_value(), "recv timed out");
    expect(*got == msg.size(), "recv size mismatch");
    return message_string(msg);
}

inline std::vector<std::string> recv_strings(zmq::socket_t &socket)
{
    std::vector<zmq::message_t> messages;
    const auto got = zmq::recv_multipart(socket, std::back_inserter(messages));
    expect(got.has_value(), "multipart recv timed out");

    std::vector<std::string> out;
    out.reserve(messages.size());
    for (const auto &msg : messages) {
        out.push_back(message_string(msg));
    }
    return out;
}

inline void expect_payload(const std::string &actual, const char *expected)
{
    expect(actual == expected, "payload mismatch");
}

inline void expect_socket_type(zmq::socket_t &socket, zmq::socket_type expected)
{
    expect(socket.get(zmq::sockopt::socket_type) == expected,
           "socket type mismatch");
}

inline void expect_size(size_t actual, size_t expected)
{
    expect(actual == expected, "part count mismatch");
}

inline void print_passed(const char *name)
{
    int major = 0;
    int minor = 0;
    int patch = 0;
    zmq::version(&major, &minor, &patch);
    std::cout << "cppzmq " << name << " passed: " << major << '.' << minor
              << '.' << patch << '\n';
}
} // namespace cppzmq_tests
