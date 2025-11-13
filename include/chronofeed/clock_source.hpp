#pragma once

#include "chronofeed/time.hpp"

namespace chronofeed {

// Source of wall-clock time used to stamp samples. Implementations must be safe to call
// concurrently from the hot path (lock-free, no syscalls beyond clock_gettime).
class WallClockSource {
 public:
  virtual ~WallClockSource() = default;
  virtual nanos_t now_wall_ns() const = 0;
  // True once the source has a trustworthy estimate (e.g. NTP has converged).
  virtual bool synchronized() const = 0;
};

// Plain CLOCK_REALTIME, used when NTP discipline is disabled.
class SystemWallClock final : public WallClockSource {
 public:
  nanos_t now_wall_ns() const override { return realtime_now(); }
  bool synchronized() const override { return false; }
};

}  // namespace chronofeed
