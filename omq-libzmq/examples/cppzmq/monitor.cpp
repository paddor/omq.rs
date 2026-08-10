#ifndef ZMQ_BUILD_DRAFT_API
#define ZMQ_BUILD_DRAFT_API
#endif

#include <zmq.hpp>

#include <chrono>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>

class ExampleMonitor : public zmq::monitor_t
{
  public:
    bool wait_for(int event)
    {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            if (seen_.count(event) != 0) {
                return true;
            }
            check_event(100);
        }
        return false;
    }

    void on_event_listening(const zmq_event_t &, const char *addr) override
    {
        seen_.insert(ZMQ_EVENT_LISTENING);
        std::cout << "monitor: listening " << addr << '\n';
    }

  private:
    std::set<int> seen_;
};

int main()
{
    try {
        zmq::context_t ctx(1);
        zmq::socket_t pull(ctx, zmq::socket_type::pull);
        pull.set(zmq::sockopt::linger, 0);

        ExampleMonitor monitor;
        monitor.init(pull, "inproc://cppzmq-example-monitor", ZMQ_EVENT_ALL);

        pull.bind("tcp://127.0.0.1:*");
        if (!monitor.wait_for(ZMQ_EVENT_LISTENING)) {
            throw std::runtime_error("missing LISTENING monitor event");
        }

        monitor.abort();
        std::cout << "cppzmq monitor passed\n";
    } catch (const zmq::error_t &e) {
        std::cerr << "zmq error: " << e.what() << '\n';
        return 1;
    } catch (const std::exception &e) {
        std::cerr << "error: " << e.what() << '\n';
        return 1;
    }
}
