#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "chronofeed/time.hpp"

// SNTPv4 client (RFC 4330). Only the client side of the on-wire protocol is
// implemented; everything is expressed in UNIX-epoch nanoseconds internally.
namespace chronofeed::ntp {

constexpr std::size_t kPacketSize = 48;
using Packet = std::array<std::uint8_t, kPacketSize>;

// Seconds between the NTP era-0 epoch (1900-01-01) and the UNIX epoch (1970-01-01).
constexpr std::int64_t kNtpUnixOffsetS = 2'208'988'800LL;

// 64-bit NTP timestamp (32.32 fixed point seconds since 1900) <-> UNIX ns.
std::uint64_t unix_ns_to_ntp(nanos_t unix_ns);
nanos_t ntp_to_unix_ns(std::uint64_t ntp);

// Client request with the transmit timestamp set to `t1_unix_ns`.
Packet build_request(nanos_t t1_unix_ns);

struct ServerReply {
  std::uint8_t leap = 0;
  std::uint8_t version = 0;
  std::uint8_t mode = 0;
  std::uint8_t stratum = 0;
  std::uint64_t originate_ts = 0;  // echo of our t1 (NTP format)
  nanos_t t2_unix_ns = 0;          // server receive
  nanos_t t3_unix_ns = 0;          // server transmit
};

// Validates and decodes a server reply. Rejects kiss-o'-death (stratum 0), unsynchronised
// servers (leap = 3), wrong mode, and replies whose originate timestamp does not echo
// `expected_originate` (protects against stale or spoofed packets).
std::optional<ServerReply> parse_reply(const Packet& pkt, std::uint64_t expected_originate);

struct Measurement {
  nanos_t offset_ns = 0;  // server_time - local_time
  nanos_t delay_ns = 0;   // round-trip network delay
  std::uint8_t stratum = 0;
};

// Standard on-wire calculation:
//   offset = ((t2 - t1) + (t3 - t4)) / 2
//   delay  =  (t4 - t1) - (t3 - t2)
Measurement compute(nanos_t t1, nanos_t t2, nanos_t t3, nanos_t t4);

// Performs one blocking UDP exchange. `local_wall` supplies t1/t4, so the returned
// offset is relative to whichever clock the caller is disciplining.
std::optional<Measurement> query(const std::string& server, std::uint16_t port, int timeout_ms,
                                 const std::function<nanos_t()>& local_wall);

}  // namespace chronofeed::ntp
