#include "chronofeed/ntp/sntp.hpp"

#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "chronofeed/log.hpp"

namespace chronofeed::ntp {

namespace {

void put_u64(std::uint8_t* p, std::uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    p[i] = static_cast<std::uint8_t>(v & 0xFF);
    v >>= 8;
  }
}

std::uint64_t get_u64(const std::uint8_t* p) {
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
  return v;
}

}  // namespace

std::uint64_t unix_ns_to_ntp(nanos_t unix_ns) {
  const std::int64_t secs = unix_ns / kNsPerSec + kNtpUnixOffsetS;
  const std::int64_t frac_ns = unix_ns % kNsPerSec;
  const std::uint64_t frac = (static_cast<std::uint64_t>(frac_ns) << 32) / static_cast<std::uint64_t>(kNsPerSec);
  return (static_cast<std::uint64_t>(secs) << 32) | (frac & 0xFFFFFFFFULL);
}

nanos_t ntp_to_unix_ns(std::uint64_t ntp) {
  const std::int64_t secs = static_cast<std::int64_t>(ntp >> 32) - kNtpUnixOffsetS;
  const std::uint64_t frac = ntp & 0xFFFFFFFFULL;
  const nanos_t frac_ns = static_cast<nanos_t>((frac * static_cast<std::uint64_t>(kNsPerSec)) >> 32);
  return secs * kNsPerSec + frac_ns;
}

Packet build_request(nanos_t t1_unix_ns) {
  Packet p{};
  p[0] = (0 << 6) | (4 << 3) | 3;  // LI = 0, VN = 4, Mode = 3 (client)
  put_u64(p.data() + 40, unix_ns_to_ntp(t1_unix_ns));
  return p;
}

std::optional<ServerReply> parse_reply(const Packet& pkt, std::uint64_t expected_originate) {
  ServerReply r;
  r.leap = pkt[0] >> 6;
  r.version = (pkt[0] >> 3) & 0x07;
  r.mode = pkt[0] & 0x07;
  r.stratum = pkt[1];

  if (r.mode != 4) return std::nullopt;                   // must be "server"
  if (r.version < 3 || r.version > 4) return std::nullopt;
  if (r.leap == 3) return std::nullopt;                   // server not synchronised
  if (r.stratum == 0 || r.stratum > 15) return std::nullopt;  // KoD or invalid

  r.originate_ts = get_u64(pkt.data() + 24);
  if (r.originate_ts != expected_originate) return std::nullopt;

  const std::uint64_t rx = get_u64(pkt.data() + 32);
  const std::uint64_t tx = get_u64(pkt.data() + 40);
  if (rx == 0 || tx == 0) return std::nullopt;
  r.t2_unix_ns = ntp_to_unix_ns(rx);
  r.t3_unix_ns = ntp_to_unix_ns(tx);
  return r;
}

Measurement compute(nanos_t t1, nanos_t t2, nanos_t t3, nanos_t t4) {
  Measurement m;
  m.offset_ns = ((t2 - t1) + (t3 - t4)) / 2;
  m.delay_ns = (t4 - t1) - (t3 - t2);
  if (m.delay_ns < 0) m.delay_ns = 0;
  return m;
}

std::optional<Measurement> query(const std::string& server, std::uint16_t port, int timeout_ms,
                                 const std::function<nanos_t()>& local_wall) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  addrinfo* res = nullptr;
  const std::string port_s = std::to_string(port);
  if (getaddrinfo(server.c_str(), port_s.c_str(), &hints, &res) != 0 || res == nullptr) {
    CF_LOG_WARN("ntp: cannot resolve %s", server.c_str());
    return std::nullopt;
  }

  const int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (fd < 0) {
    freeaddrinfo(res);
    return std::nullopt;
  }
  // connect() on UDP filters replies to the chosen server address.
  if (::connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
    freeaddrinfo(res);
    ::close(fd);
    return std::nullopt;
  }
  freeaddrinfo(res);

  const nanos_t t1 = local_wall();
  const Packet req = build_request(t1);
  const std::uint64_t originate = unix_ns_to_ntp(t1);

  std::optional<Measurement> out;
  if (::send(fd, req.data(), req.size(), 0) == static_cast<ssize_t>(req.size())) {
    pollfd pfd{fd, POLLIN, 0};
    if (::poll(&pfd, 1, timeout_ms) > 0) {
      Packet reply{};
      const ssize_t n = ::recv(fd, reply.data(), reply.size(), 0);
      const nanos_t t4 = local_wall();
      if (n == static_cast<ssize_t>(kPacketSize)) {
        if (auto r = parse_reply(reply, originate)) {
          Measurement m = compute(t1, r->t2_unix_ns, r->t3_unix_ns, t4);
          m.stratum = r->stratum;
          out = m;
        } else {
          CF_LOG_DEBUG("ntp: rejected reply from %s", server.c_str());
        }
      }
    } else {
      CF_LOG_DEBUG("ntp: timeout waiting for %s", server.c_str());
    }
  }
  ::close(fd);
  return out;
}

}  // namespace chronofeed::ntp
