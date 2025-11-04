#include "chronofeed/mqtt/client.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>

#include "chronofeed/log.hpp"

namespace chronofeed::mqtt {

Client::Client(MqttConfig cfg, PublishHandler& handler) : cfg_(std::move(cfg)), handler_(handler) {}

Client::~Client() { close_socket(); }

void Client::stop() { running_.store(false, std::memory_order_release); }

bool Client::open_socket() {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  const std::string port = std::to_string(cfg_.port);
  const int rc = getaddrinfo(cfg_.host.c_str(), port.c_str(), &hints, &res);
  if (rc != 0) {
    CF_LOG_WARN("mqtt: resolve %s failed: %s", cfg_.host.c_str(), gai_strerror(rc));
    return false;
  }

  for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    const int fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) continue;
    if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
      // Disable Nagle: small sensor frames must not be batched by the kernel.
      int one = 1;
      setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_BUSY_POLL
      // Opportunistic low-latency receive on Linux; ignored if unsupported.
      int busy_us = 50;
      setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &busy_us, sizeof(busy_us));
#endif
      fd_ = fd;
      break;
    }
    ::close(fd);
  }
  freeaddrinfo(res);
  if (fd_ < 0) {
    CF_LOG_WARN("mqtt: connect to %s:%u failed: %s", cfg_.host.c_str(), cfg_.port, std::strerror(errno));
    return false;
  }
  return true;
}

void Client::close_socket() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  connected_.store(false, std::memory_order_release);
  buf_len_ = 0;
  ping_outstanding_ = false;
}

bool Client::send_all(const std::vector<std::uint8_t>& bytes) {
  std::size_t off = 0;
  while (off < bytes.size()) {
#ifdef MSG_NOSIGNAL
    const ssize_t n = ::send(fd_, bytes.data() + off, bytes.size() - off, MSG_NOSIGNAL);
#else
    const ssize_t n = ::send(fd_, bytes.data() + off, bytes.size() - off, 0);
#endif
    if (n < 0) {
      if (errno == EINTR) continue;
      CF_LOG_WARN("mqtt: send failed: %s", std::strerror(errno));
      return false;
    }
    off += static_cast<std::size_t>(n);
  }
  last_tx_mono_ = mono_now();
  return true;
}

bool Client::read_some(int timeout_ms, nanos_t& rx_mono_ns) {
  pollfd pfd{fd_, POLLIN, 0};
  const int pr = ::poll(&pfd, 1, timeout_ms);
  if (pr < 0) return errno == EINTR;
  if (pr == 0) {
    rx_mono_ns = 0;  // timeout, nothing read
    return true;
  }
  if (buf_len_ == buf_.size()) {
    CF_LOG_ERROR("mqtt: receive buffer overflow (packet larger than %zu bytes)", buf_.size());
    return false;
  }
  const ssize_t n = ::recv(fd_, buf_.data() + buf_len_, buf_.size() - buf_len_, 0);
  rx_mono_ns = mono_now();
  if (n == 0) {
    CF_LOG_WARN("mqtt: broker closed connection");
    return false;
  }
  if (n < 0) {
    if (errno == EINTR || errno == EAGAIN) {
      rx_mono_ns = 0;
      return true;
    }
    CF_LOG_WARN("mqtt: recv failed: %s", std::strerror(errno));
    return false;
  }
  buf_len_ += static_cast<std::size_t>(n);
  stats_.bytes.fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed);
  return true;
}

bool Client::await_packet(PacketType want, RawPacket& out, int timeout_ms) {
  const nanos_t give_up = mono_now() + static_cast<nanos_t>(timeout_ms) * kNsPerMs;
  while (mono_now() < give_up && running_.load(std::memory_order_acquire)) {
    const auto st = frame_packet(buf_.data(), buf_len_, out);
    if (st == DecodeStatus::Malformed) return false;
    if (st == DecodeStatus::Ok) {
      if (out.type == want) return true;
      // CONNACK is always the first packet from a compliant broker; skip anything else.
      std::memmove(buf_.data(), buf_.data() + out.total_len, buf_len_ - out.total_len);
      buf_len_ -= out.total_len;
      continue;
    }
    nanos_t rx = 0;
    if (!read_some(100, rx)) return false;
  }
  return false;
}

bool Client::connect_and_subscribe() {
  if (!open_socket()) return false;

  ConnectOptions opts;
  opts.client_id = cfg_.client_id;
  opts.username = cfg_.username;
  opts.password = cfg_.password;
  opts.keepalive_s = cfg_.keepalive_s;
  if (!send_all(encode_connect(opts))) return false;

  RawPacket pkt;
  Connack ack;
  if (!await_packet(PacketType::Connack, pkt, 5000) || !parse_connack(pkt, ack)) {
    CF_LOG_WARN("mqtt: no valid CONNACK from broker");
    return false;
  }
  std::memmove(buf_.data(), buf_.data() + pkt.total_len, buf_len_ - pkt.total_len);
  buf_len_ -= pkt.total_len;
  if (ack.return_code != 0) {
    CF_LOG_ERROR("mqtt: connection refused: %s", connack_reason(ack.return_code));
    return false;
  }

  const std::uint16_t pid = next_packet_id_++;
  if (next_packet_id_ == 0) next_packet_id_ = 1;
  if (!send_all(encode_subscribe(pid, cfg_.topic, 0))) return false;

  // SUBACK is consumed by pump() like any other packet; publishes may race ahead of it.
  connected_.store(true, std::memory_order_release);
  CF_LOG_INFO("mqtt: connected to %s:%u, subscribed to '%s'", cfg_.host.c_str(), cfg_.port,
              cfg_.topic.c_str());
  return true;
}

bool Client::drain_buffer(nanos_t rx_mono_ns) {
  std::size_t off = 0;
  while (off < buf_len_) {
    RawPacket pkt;
    const auto st = frame_packet(buf_.data() + off, buf_len_ - off, pkt);
    if (st == DecodeStatus::NeedMore) break;
    if (st == DecodeStatus::Malformed) {
      stats_.malformed.fetch_add(1, std::memory_order_relaxed);
      CF_LOG_WARN("mqtt: malformed packet, dropping connection");
      return false;
    }

    switch (pkt.type) {
      case PacketType::Publish: {
        PublishView view;
        if (parse_publish(pkt, view)) {
          stats_.messages.fetch_add(1, std::memory_order_relaxed);
          handler_.on_publish(view, rx_mono_ns);
          if (view.qos == 1) {
            if (!send_all(encode_puback(view.packet_id))) return false;
          }
        } else {
          stats_.malformed.fetch_add(1, std::memory_order_relaxed);
        }
        break;
      }
      case PacketType::Suback: {
        Suback sa;
        if (parse_suback(pkt, sa) && sa.granted_qos == 0x80) {
          CF_LOG_ERROR("mqtt: broker rejected subscription to '%s'", cfg_.topic.c_str());
          return false;
        }
        break;
      }
      case PacketType::Pingresp:
        ping_outstanding_ = false;
        break;
      default:
        CF_LOG_DEBUG("mqtt: ignoring packet type %d", static_cast<int>(pkt.type));
        break;
    }
    off += pkt.total_len;
  }

  if (off > 0) {
    std::memmove(buf_.data(), buf_.data() + off, buf_len_ - off);
    buf_len_ -= off;
  }
  return true;
}

void Client::pump() {
  const nanos_t keepalive_ns = static_cast<nanos_t>(cfg_.keepalive_s) * kNsPerSec;
  // Packets already buffered during the handshake (e.g. retained messages).
  if (!drain_buffer(mono_now())) return;

  while (running_.load(std::memory_order_acquire)) {
    nanos_t rx = 0;
    if (!read_some(100, rx)) return;
    if (rx != 0 && !drain_buffer(rx)) return;

    const nanos_t now = mono_now();
    if (keepalive_ns > 0) {
      if (ping_outstanding_ && now - ping_sent_mono_ > keepalive_ns) {
        CF_LOG_WARN("mqtt: PINGRESP timeout");
        return;
      }
      // Ping at 3/4 of the keepalive so the broker never sees us as idle.
      if (!ping_outstanding_ && now - last_tx_mono_ > keepalive_ns * 3 / 4) {
        if (!send_all(encode_pingreq())) return;
        ping_outstanding_ = true;
        ping_sent_mono_ = now;
      }
    }
  }
}

void Client::run() {
  running_.store(true, std::memory_order_release);
  int backoff_ms = std::max(50, cfg_.reconnect_backoff_ms);
  bool first = true;

  while (running_.load(std::memory_order_acquire)) {
    if (!first) stats_.reconnects.fetch_add(1, std::memory_order_relaxed);
    first = false;

    if (connect_and_subscribe()) {
      backoff_ms = std::max(50, cfg_.reconnect_backoff_ms);
      pump();
      if (fd_ >= 0 && !running_.load(std::memory_order_acquire)) send_all(encode_disconnect());
    }
    close_socket();

    if (!running_.load(std::memory_order_acquire)) break;
    CF_LOG_INFO("mqtt: reconnecting in %d ms", backoff_ms);
    // Sleep in small slices so stop() is responsive.
    const nanos_t wake = mono_now() + static_cast<nanos_t>(backoff_ms) * kNsPerMs;
    while (running_.load(std::memory_order_acquire) && mono_now() < wake) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    backoff_ms = std::min(backoff_ms * 2, 10'000);
  }
}

}  // namespace chronofeed::mqtt
