#pragma once

#include <cstdint>
#include <ctime>

namespace chronofeed {

// All internal time arithmetic is done in signed 64-bit nanoseconds.
using nanos_t = std::int64_t;

constexpr nanos_t kNsPerUs = 1'000;
constexpr nanos_t kNsPerMs = 1'000'000;
constexpr nanos_t kNsPerSec = 1'000'000'000;

inline nanos_t to_nanos(const timespec& ts) {
  return static_cast<nanos_t>(ts.tv_sec) * kNsPerSec + ts.tv_nsec;
}

inline timespec to_timespec(nanos_t ns) {
  timespec ts{};
  ts.tv_sec = static_cast<time_t>(ns / kNsPerSec);
  ts.tv_nsec = static_cast<long>(ns % kNsPerSec);
  if (ts.tv_nsec < 0) {
    ts.tv_nsec += kNsPerSec;
    ts.tv_sec -= 1;
  }
  return ts;
}

// Monotonic clock: the only clock used for deadline arithmetic.
inline nanos_t mono_now() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return to_nanos(ts);
}

// Raw system wall clock (not disciplined by ChronoFeed).
inline nanos_t realtime_now() {
  timespec ts{};
  clock_gettime(CLOCK_REALTIME, &ts);
  return to_nanos(ts);
}

// Absolute sleep on the monotonic clock; robust against EINTR.
inline void sleep_until_mono(nanos_t deadline) {
  timespec ts = to_timespec(deadline);
#if defined(__linux__)
  while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr) != 0) {
  }
#else
  nanos_t now = mono_now();
  while (now < deadline) {
    timespec rel = to_timespec(deadline - now);
    nanosleep(&rel, nullptr);
    now = mono_now();
  }
  (void)ts;
#endif
}

inline double ns_to_ms(nanos_t ns) { return static_cast<double>(ns) / kNsPerMs; }
inline double ns_to_us(nanos_t ns) { return static_cast<double>(ns) / kNsPerUs; }

}  // namespace chronofeed
