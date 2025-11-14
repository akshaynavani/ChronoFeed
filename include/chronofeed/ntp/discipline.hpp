#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <thread>

#include "chronofeed/clock_source.hpp"
#include "chronofeed/config.hpp"
#include "chronofeed/ntp/sntp.hpp"
#include "chronofeed/time.hpp"

namespace chronofeed::ntp {

// A software wall clock layered on CLOCK_MONOTONIC:
//
//   wall(m) = base_wall + (m - base_mono) * (1 + freq_ppm * 1e-6)
//
// Corrections never touch the kernel clock (no root needed, no interference with other
// processes) and never move `wall` backwards by more than a deliberate step. Parameters
// are published through a seqlock so hot-path readers are wait-free in the common case
// and never take a lock.
class DisciplinedClock final : public WallClockSource {
 public:
  DisciplinedClock();

  nanos_t now_wall_ns() const override { return wall_at(mono_now()); }
  bool synchronized() const override { return synced_.load(std::memory_order_acquire); }

  nanos_t wall_at(nanos_t mono_ns) const;
  double freq_ppm() const;

  // Single writer (the NTP thread). Rebases at `mono_ns`, shifting phase by
  // `phase_ns` and installing a new frequency correction.
  void adjust(nanos_t mono_ns, nanos_t phase_ns, double freq_ppm);
  void mark_synchronized(bool v) { synced_.store(v, std::memory_order_release); }

 private:
  struct Params {
    nanos_t base_mono;
    nanos_t base_wall;
    double freq_ppm;
  };
  Params read() const;

  std::atomic<std::uint32_t> seq_{0};
  std::atomic<nanos_t> base_mono_{0};
  std::atomic<nanos_t> base_wall_{0};
  std::atomic<double> freq_ppm_{0.0};
  std::atomic<bool> synced_{false};
};

// Pure PI discipline loop, separated from I/O so it can be unit tested.
//
// - Clock filter: keeps the last N round-trip delays; samples whose delay is well above
//   the window minimum are rejected (queueing delay corrupts the offset estimate).
// - Offsets larger than `max_step_ns` are stepped in one go (initial sync or a large jump).
// - Otherwise: phase += kp * offset, freq += ki * offset / interval (the integral term
//   learns the oscillator's drift so future offsets shrink toward zero).
class ClockDiscipline {
 public:
  enum class Action { Reject, Step, Slew };

  struct Decision {
    Action action = Action::Reject;
    nanos_t phase_ns = 0;
    double freq_ppm = 0.0;
  };

  static constexpr std::size_t kFilterWindow = 8;
  static constexpr double kMaxFreqPpm = 500.0;

  explicit ClockDiscipline(const NtpConfig& cfg);

  // `interval_ns` is the monotonic time since the previous accepted measurement
  // (0 for the first one).
  Decision update(const Measurement& m, nanos_t interval_ns);

  double freq_ppm() const { return freq_ppm_; }
  std::uint64_t accepted() const { return accepted_; }
  std::uint64_t rejected() const { return rejected_; }

 private:
  bool passes_filter(nanos_t delay_ns);

  double kp_;
  double ki_;
  nanos_t max_step_ns_;
  double freq_ppm_ = 0.0;
  std::array<nanos_t, kFilterWindow> delays_{};
  std::size_t delay_count_ = 0;
  std::size_t delay_head_ = 0;
  std::uint64_t accepted_ = 0;
  std::uint64_t rejected_ = 0;
};

// Background (non-RT) thread that polls the NTP server and steers a DisciplinedClock.
class NtpSynchronizer {
 public:
  NtpSynchronizer(NtpConfig cfg, DisciplinedClock& clock);
  ~NtpSynchronizer();

  void start();
  void stop();

  struct Status {
    nanos_t last_offset_ns = 0;
    nanos_t last_delay_ns = 0;
    double freq_ppm = 0.0;
    std::uint64_t accepted = 0;
    std::uint64_t rejected = 0;
    std::uint64_t failures = 0;
  };
  Status status() const;

 private:
  void loop();
  void poll_once();

  NtpConfig cfg_;
  DisciplinedClock& clock_;
  ClockDiscipline discipline_;
  std::atomic<bool> running_{false};
  std::thread thread_;
  nanos_t last_accept_mono_ = 0;

  std::atomic<nanos_t> last_offset_ns_{0};
  std::atomic<nanos_t> last_delay_ns_{0};
  std::atomic<std::uint64_t> accepted_{0};
  std::atomic<std::uint64_t> rejected_{0};
  std::atomic<std::uint64_t> failures_{0};
};

}  // namespace chronofeed::ntp
