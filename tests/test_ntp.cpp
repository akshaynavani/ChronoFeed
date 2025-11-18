#include <gtest/gtest.h>

#include <cstdlib>

#include "chronofeed/ntp/discipline.hpp"
#include "chronofeed/ntp/sntp.hpp"

using namespace chronofeed;
using namespace chronofeed::ntp;

namespace {

void put_u64(Packet& p, std::size_t off, std::uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    p[off + static_cast<std::size_t>(i)] = static_cast<std::uint8_t>(v & 0xFF);
    v >>= 8;
  }
}

Packet make_reply(std::uint64_t originate, nanos_t t2, nanos_t t3, std::uint8_t stratum = 2,
                  std::uint8_t leap = 0) {
  Packet p{};
  p[0] = static_cast<std::uint8_t>((leap << 6) | (4 << 3) | 4);
  p[1] = stratum;
  put_u64(p, 24, originate);
  put_u64(p, 32, unix_ns_to_ntp(t2));
  put_u64(p, 40, unix_ns_to_ntp(t3));
  return p;
}

}  // namespace

TEST(Sntp, TimestampConversionRoundTrips) {
  const nanos_t t = 1'700'000'000'123'456'789LL;
  const nanos_t back = ntp_to_unix_ns(unix_ns_to_ntp(t));
  EXPECT_LE(std::llabs(back - t), 1);  // 32-bit fraction has ~233 ps resolution
}

TEST(Sntp, UnixEpochMapsToNtpEraOffset) {
  EXPECT_EQ(unix_ns_to_ntp(0) >> 32, static_cast<std::uint64_t>(kNtpUnixOffsetS));
}

TEST(Sntp, ComputesOffsetAndDelay) {
  // Local clock is 10 ms behind the server; 4 ms symmetric one-way delay; 1 ms server hold.
  const nanos_t t1 = 1'000 * kNsPerMs;
  const nanos_t t2 = t1 + 4 * kNsPerMs + 10 * kNsPerMs;
  const nanos_t t3 = t2 + 1 * kNsPerMs;
  const nanos_t t4 = t3 + 4 * kNsPerMs - 10 * kNsPerMs;
  const auto m = compute(t1, t2, t3, t4);
  EXPECT_EQ(m.offset_ns, 10 * kNsPerMs);
  EXPECT_EQ(m.delay_ns, 8 * kNsPerMs);
}

TEST(Sntp, RequestIsClientModeV4) {
  const auto req = build_request(1'700'000'000LL * kNsPerSec);
  EXPECT_EQ(req[0] & 0x07, 3);
  EXPECT_EQ((req[0] >> 3) & 0x07, 4);
}

TEST(Sntp, ParseReplyValidatesOriginateAndHeader) {
  const nanos_t t1 = 1'700'000'000LL * kNsPerSec;
  const std::uint64_t org = unix_ns_to_ntp(t1);
  const nanos_t t2 = t1 + kNsPerMs;
  const nanos_t t3 = t2 + kNsPerMs;

  auto ok = parse_reply(make_reply(org, t2, t3), org);
  ASSERT_TRUE(ok.has_value());
  EXPECT_EQ(ok->stratum, 2);

  EXPECT_FALSE(parse_reply(make_reply(org + 1, t2, t3), org).has_value());          // stale/spoofed
  EXPECT_FALSE(parse_reply(make_reply(org, t2, t3, /*stratum=*/0), org).has_value());  // kiss-o'-death
  EXPECT_FALSE(parse_reply(make_reply(org, t2, t3, 2, /*leap=*/3), org).has_value()); // unsynced
}

TEST(ClockDiscipline, FirstSampleAndLargeOffsetsStep) {
  NtpConfig cfg;
  ClockDiscipline d(cfg);
  auto r = d.update({2 * kNsPerMs, 10 * kNsPerMs, 2}, 0);
  EXPECT_EQ(r.action, ClockDiscipline::Action::Step);
  EXPECT_EQ(r.phase_ns, 2 * kNsPerMs);

  r = d.update({500 * kNsPerMs, 10 * kNsPerMs, 2}, 16 * kNsPerSec);
  EXPECT_EQ(r.action, ClockDiscipline::Action::Step);
}

TEST(ClockDiscipline, RejectsHighDelayOutliers) {
  NtpConfig cfg;
  ClockDiscipline d(cfg);
  for (int i = 0; i < 4; ++i) d.update({0, 5 * kNsPerMs, 2}, kNsPerSec);
  const auto r = d.update({3 * kNsPerMs, 80 * kNsPerMs, 2}, kNsPerSec);
  EXPECT_EQ(r.action, ClockDiscipline::Action::Reject);
  EXPECT_EQ(d.rejected(), 1u);
}

TEST(ClockDiscipline, LearnsOscillatorDriftAndConverges) {
  // Local oscillator runs 50 ppm fast relative to the server.
  NtpConfig cfg;
  ClockDiscipline d(cfg);
  const double drift_ppm = 50.0;
  const nanos_t interval = 16 * kNsPerSec;
  double err_ns = 3.0 * kNsPerMs;  // local - true
  double freq = 0.0;

  for (int k = 0; k < 200; ++k) {
    Measurement m{static_cast<nanos_t>(-err_ns), 5 * kNsPerMs, 2};
    const auto dec = d.update(m, k == 0 ? 0 : interval);
    ASSERT_NE(dec.action, ClockDiscipline::Action::Reject);
    err_ns += static_cast<double>(dec.phase_ns);
    freq = dec.freq_ppm;
    err_ns += (drift_ppm + freq) * 1e-6 * static_cast<double>(interval);
  }
  EXPECT_NEAR(freq, -drift_ppm, 0.5);
  EXPECT_LT(std::abs(err_ns), 50.0 * kNsPerUs);
}

TEST(DisciplinedClock, AdjustAppliesPhaseAndFrequency) {
  DisciplinedClock c;
  const nanos_t m0 = mono_now();
  const nanos_t before = c.wall_at(m0);
  c.adjust(m0, 1 * kNsPerMs, 100.0);
  EXPECT_EQ(c.wall_at(m0), before + kNsPerMs);
  // 1 s later with +100 ppm => +100 us extra.
  EXPECT_NEAR(static_cast<double>(c.wall_at(m0 + kNsPerSec) - c.wall_at(m0)),
              static_cast<double>(kNsPerSec + 100 * kNsPerUs), 2.0);
  EXPECT_DOUBLE_EQ(c.freq_ppm(), 100.0);
}
