#include "common.hpp"

#include <set>

using namespace cppzmq_tests;

namespace {
class RecordingMonitor : public zmq::monitor_t
{
  public:
    bool wait_for(std::initializer_list<int> wanted)
    {
        const std::set<int> target(wanted);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);

        while (std::chrono::steady_clock::now() < deadline) {
            bool complete = true;
            for (int event : target) {
                if (events.count(event) == 0) {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                return true;
            }
            check_event(100);
        }
        return false;
    }

    void on_event_listening(const zmq_event_t &event, const char *addr) override
    {
        (void)event;
        events.insert(ZMQ_EVENT_LISTENING);
        listening_endpoint = addr;
    }

    void on_event_accepted(const zmq_event_t &event, const char *addr) override
    {
        (void)event;
        (void)addr;
        events.insert(ZMQ_EVENT_ACCEPTED);
    }

    void on_event_handshake_succeeded(const zmq_event_t &event,
                                      const char *addr) override
    {
        (void)event;
        (void)addr;
        events.insert(ZMQ_EVENT_HANDSHAKE_SUCCEEDED);
    }

    std::set<int> events;
    std::string listening_endpoint;
};
} // namespace

int main()
{
    zmq::context_t ctx(1);
    zmq::socket_t pull(ctx, zmq::socket_type::pull);
    set_timeouts(pull);

    RecordingMonitor monitor;
    monitor.init(pull, "inproc://cppzmq-monitor", ZMQ_EVENT_ALL);

    const std::string endpoint = bind_random_tcp(pull);
    expect(monitor.wait_for({ZMQ_EVENT_LISTENING}), "missing LISTENING event");
    expect(monitor.listening_endpoint == endpoint, "LISTENING endpoint mismatch");

    zmq::socket_t push(ctx, zmq::socket_type::push);
    set_timeouts(push);
    push.connect(endpoint);

    expect(monitor.wait_for(
               {ZMQ_EVENT_LISTENING, ZMQ_EVENT_ACCEPTED,
                ZMQ_EVENT_HANDSHAKE_SUCCEEDED}),
           "missing accepted or handshake monitor event");

    send_text(push, "monitor");
    expect_payload(recv_string(pull), "monitor");

    monitor.abort();
    print_passed("monitor");
    return 0;
}
