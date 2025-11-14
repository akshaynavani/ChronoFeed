// chronofeed_pub: deterministic MQTT load generator for exercising the pipeline.
//
// Publishes synthetic sensor readings (sine wave + noise) at a fixed aggregate rate,
// paced on absolute monotonic deadlines so the offered load does not drift.

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "chronofeed/mqtt/packet.hpp"
#include "chronofeed/time.hpp"

using namespace chronofeed;

namespace {

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true); }

struct Options {
  std::string host = "127.0.0.1";
  std::uint16_t port = 1883;
  std::string topic_prefix = "sensors";
  int sensors = 8;
  int rate_hz = 1000;  // aggregate messages per second
  int duration_s = 10;
  bool json = false;
  int ooo_every = 0;   // inject a sequence gap every N messages (0 = never)
};

void usage(const char* argv0) {
  std::fprintf(stderr,
               "usage: %s [-H host] [-p port] [-t topic_prefix] [-n sensors] [-r rate_hz]\n"
               "          [-d duration_s] [-j] [-g gap_every]\n"
               "  -j  publish JSON payloads instead of CSV\n"
               "  -g  skip a sequence number every N messages (exercises gap detection)\n"
               "Credentials: CHRONOFEED_MQTT_USERNAME / CHRONOFEED_MQTT_PASSWORD\n",
               argv0);
}

int connect_tcp(const std::string& host, std::uint16_t port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) return -1;
  int fd = -1;
  for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
    fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) continue;
    if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;
    ::close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd >= 0) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }
  return fd;
}

bool send_all(int fd, const std::vector<std::uint8_t>& b) {
  std::size_t off = 0;
  while (off < b.size()) {
    const ssize_t n = ::send(fd, b.data() + off, b.size() - off, 0);
    if (n <= 0) {
      if (n < 0 && errno == EINTR) continue;
      return false;
    }
    off += static_cast<std::size_t>(n);
  }
  return true;
}

bool await_connack(int fd) {
  std::uint8_t buf[16];
  std::size_t len = 0;
  pollfd pfd{fd, POLLIN, 0};
  while (len < sizeof(buf) && ::poll(&pfd, 1, 5000) > 0) {
    const ssize_t n = ::recv(fd, buf + len, sizeof(buf) - len, 0);
    if (n <= 0) return false;
    len += static_cast<std::size_t>(n);
    mqtt::RawPacket pkt;
    if (mqtt::frame_packet(buf, len, pkt) == mqtt::DecodeStatus::Ok) {
      mqtt::Connack ack;
      if (!mqtt::parse_connack(pkt, ack)) return false;
      if (ack.return_code != 0) {
        std::fprintf(stderr, "publisher: broker refused: %s\n", mqtt::connack_reason(ack.return_code));
        return false;
      }
      return true;
    }
  }
  return false;
}

// Drain anything the broker sends (PINGRESP etc.) so its buffers never fill.
void drain(int fd) {
  std::uint8_t sink[512];
  while (::recv(fd, sink, sizeof(sink), MSG_DONTWAIT) > 0) {
  }
}

}  // namespace

int main(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) {
        usage(argv[0]);
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "-H") o.host = next();
    else if (a == "-p") o.port = static_cast<std::uint16_t>(std::atoi(next()));
    else if (a == "-t") o.topic_prefix = next();
    else if (a == "-n") o.sensors = std::max(1, std::atoi(next()));
    else if (a == "-r") o.rate_hz = std::max(1, std::atoi(next()));
    else if (a == "-d") o.duration_s = std::atoi(next());
    else if (a == "-j") o.json = true;
    else if (a == "-g") o.ooo_every = std::atoi(next());
    else {
      usage(argv[0]);
      return a == "-h" ? 0 : 2;
    }
  }

  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);
  signal(SIGPIPE, SIG_IGN);

  const int fd = connect_tcp(o.host, o.port);
  if (fd < 0) {
    std::fprintf(stderr, "publisher: cannot connect to %s:%u\n", o.host.c_str(), o.port);
    return 1;
  }

  mqtt::ConnectOptions co;
  co.client_id = "chronofeed-pub-" + std::to_string(getpid());
  co.keepalive_s = 60;
  if (const char* u = std::getenv("CHRONOFEED_MQTT_USERNAME")) co.username = u;
  if (const char* p = std::getenv("CHRONOFEED_MQTT_PASSWORD")) co.password = p;
  if (!send_all(fd, mqtt::encode_connect(co)) || !await_connack(fd)) {
    std::fprintf(stderr, "publisher: MQTT handshake failed\n");
    ::close(fd);
    return 1;
  }

  std::mt19937_64 rng(42);
  std::normal_distribution<double> noise(0.0, 0.05);
  std::vector<std::uint64_t> seq(static_cast<std::size_t>(o.sensors), 0);
  std::vector<std::string> topics;
  for (int s = 0; s < o.sensors; ++s) topics.push_back(o.topic_prefix + "/sensor-" + std::to_string(s));

  const nanos_t period = kNsPerSec / o.rate_hz;
  const nanos_t start = mono_now();
  const nanos_t end = o.duration_s > 0 ? start + static_cast<nanos_t>(o.duration_s) * kNsPerSec : INT64_MAX;
  nanos_t next = start;
  nanos_t last_ping = start;
  std::uint64_t sent = 0;
  char payload[256];

  std::fprintf(stderr, "publisher: %d sensors, %d msg/s, %s payloads -> %s:%u/%s/#\n", o.sensors, o.rate_hz,
               o.json ? "JSON" : "CSV", o.host.c_str(), o.port, o.topic_prefix.c_str());

  while (!g_stop.load() && next < end) {
    sleep_until_mono(next);
    next += period;

    const int s = static_cast<int>(sent % static_cast<std::uint64_t>(o.sensors));
    auto& sq = seq[static_cast<std::size_t>(s)];
    ++sq;
    if (o.ooo_every > 0 && sent > 0 && sent % static_cast<std::uint64_t>(o.ooo_every) == 0) ++sq;

    const double t = static_cast<double>(mono_now() - start) / kNsPerSec;
    const double value = 20.0 + 5.0 * std::sin(2.0 * M_PI * 0.2 * t + s) + noise(rng);
    const nanos_t ts = realtime_now();

    int n;
    if (o.json) {
      n = std::snprintf(payload, sizeof(payload), "{\"id\":\"sensor-%d\",\"seq\":%llu,\"ts\":%lld,\"value\":%.5f}", s,
                        static_cast<unsigned long long>(sq), static_cast<long long>(ts), value);
    } else {
      n = std::snprintf(payload, sizeof(payload), "sensor-%d,%llu,%lld,%.5f", s,
                        static_cast<unsigned long long>(sq), static_cast<long long>(ts), value);
    }
    if (!send_all(fd, mqtt::encode_publish(topics[static_cast<std::size_t>(s)],
                                           std::string_view(payload, static_cast<std::size_t>(n))))) {
      std::fprintf(stderr, "publisher: send failed after %llu messages\n", static_cast<unsigned long long>(sent));
      break;
    }
    ++sent;

    const nanos_t now = mono_now();
    if (now - last_ping > 30 * kNsPerSec) {
      send_all(fd, mqtt::encode_pingreq());
      last_ping = now;
    }
    drain(fd);
  }

  send_all(fd, mqtt::encode_disconnect());
  ::close(fd);
  const double elapsed = static_cast<double>(mono_now() - start) / kNsPerSec;
  std::fprintf(stderr, "publisher: sent %llu messages in %.2fs (%.0f msg/s)\n", static_cast<unsigned long long>(sent),
               elapsed, elapsed > 0 ? static_cast<double>(sent) / elapsed : 0.0);
  return 0;
}
