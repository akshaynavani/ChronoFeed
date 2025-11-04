#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "chronofeed/config.hpp"
#include "chronofeed/mqtt/packet.hpp"
#include "chronofeed/time.hpp"

namespace chronofeed::mqtt {

// Receives PUBLISH messages on the client's thread. `rx_mono_ns` is the monotonic time
// at which the bytes carrying the message were read off the socket, which is the start
// of the end-to-end deadline window.
class PublishHandler {
 public:
  virtual ~PublishHandler() = default;
  virtual void on_publish(const PublishView& msg, nanos_t rx_mono_ns) = 0;
};

struct ClientStats {
  std::atomic<std::uint64_t> messages{0};
  std::atomic<std::uint64_t> bytes{0};
  std::atomic<std::uint64_t> reconnects{0};
  std::atomic<std::uint64_t> malformed{0};
};

// Single-threaded, blocking MQTT 3.1.1 subscriber. `run()` owns the socket and is
// meant to be the body of the pinned ingest thread. The receive buffer is a fixed
// array, so steady-state operation performs no heap allocation.
class Client {
 public:
  Client(MqttConfig cfg, PublishHandler& handler);
  ~Client();

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  // Connects, subscribes, and pumps messages until `stop()` is called.
  // Reconnects with exponential backoff on any socket or protocol error.
  void run();
  void stop();

  const ClientStats& stats() const { return stats_; }
  bool connected() const { return connected_.load(std::memory_order_acquire); }

 private:
  bool connect_and_subscribe();
  bool open_socket();
  void close_socket();
  bool send_all(const std::vector<std::uint8_t>& bytes);
  // Reads until one complete packet of `want` type is framed, or timeout.
  bool await_packet(PacketType want, RawPacket& out, int timeout_ms);
  // Pump loop; returns when the connection is lost or stop requested.
  void pump();
  // Frames and dispatches every complete packet currently in the buffer.
  bool drain_buffer(nanos_t rx_mono_ns);
  bool read_some(int timeout_ms, nanos_t& rx_mono_ns);

  MqttConfig cfg_;
  PublishHandler& handler_;
  int fd_ = -1;
  std::atomic<bool> running_{false};
  std::atomic<bool> connected_{false};
  ClientStats stats_;

  static constexpr std::size_t kBufSize = 64 * 1024;
  std::array<std::uint8_t, kBufSize> buf_{};
  std::size_t buf_len_ = 0;
  nanos_t last_tx_mono_ = 0;
  bool ping_outstanding_ = false;
  nanos_t ping_sent_mono_ = 0;
  std::uint16_t next_packet_id_ = 1;
};

}  // namespace chronofeed::mqtt
