#include "chronofeed/ntp/discipline.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>

#include "chronofeed/log.hpp"
#include "chronofeed/rt/thread.hpp"

namespace chronofeed::ntp {

// --- DisciplinedClock ----------------------------------------------------------

DisciplinedClock::DisciplinedClock() {
  // Seed from the system wall clock so readings are sane before the first NTP reply.
  base_mono_.store(mono_now(), std::memory_order_relaxed);
  base_wall_.store(realtime_now(), std::memory_order_relaxed);
}

DisciplinedClock::Params DisciplinedClock::read() const {
  Params p{};
  while (true) {
    const std::uint32_t s1 = seq_.load(std::memory_order_acquire);
    if (s1 & 1U) {
      rt::cpu_relax();
      continue;  // writer in progress
    }
    p.base_mono = base_mono_.load(std::memory_order_relaxed);
    p.base_wall = base_wall_.load(std::memory_order_relaxed);
    p.freq_ppm = freq_ppm_.load(std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_acquire);
    if (seq_.load(std::memory_order_relaxed) == s1) return p;
  }
}

nanos_t DisciplinedClock::wall_at(nanos_t mono_ns) const {
  const Params p = read();
  const nanos_t elapsed = mono_ns - p.base_mono;
  const double corr = static_cast<double>(elapsed) * p.freq_ppm * 1e-6;
  return p.base_wall + elapsed + static_cast<nanos_t>(std::llround(corr));
}

double DisciplinedClock::freq_ppm() const { return read().freq_ppm; }

void DisciplinedClock::adjust(nanos_t mono_ns, nanos_t phase_ns, double freq_ppm) {
  const nanos_t wall_now = wall_at(mono_ns);
  const std::uint32_t s = seq_.load(std::memory_order_relaxed);
  seq_.store(s + 1, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_release);
  base_mono_.store(mono_ns, std::memory_order_relaxed);
  base_wall_.store(wall_now + phase_ns, std::memory_order_relaxed);
  freq_ppm_.store(freq_ppm, std::memory_order_relaxed);
  seq_.store(s + 2, std::memory_order_release);
}

// --- ClockDiscipline -----------------------------------------------------------

ClockDiscipline::ClockDiscipline(const NtpConfig& cfg)
    : kp_(cfg.kp), ki_(cfg.ki), max_step_ns_(cfg.max_step_ns) {}

bool ClockDiscipline::passes_filter(nanos_t delay_ns) {
  delays_[delay_head_] = delay_ns;
  delay_head_ = (delay_head_ + 1) % kFilterWindow;
  if (delay_count_ < kFilterWindow) ++delay_count_;

  // Need a few samples before the filter has a meaningful baseline.
  if (delay_count_ < 3) return true;
  const nanos_t min_delay = *std::min_element(delays_.begin(), delays_.begin() + static_cast<std::ptrdiff_t>(delay_count_));
  // Accept if within 2x the best observed path plus 1 ms of slack for jitter.
  return delay_ns <= 2 * min_delay + kNsPerMs;
}

ClockDiscipline::Decision ClockDiscipline::update(const Measurement& m, nanos_t interval_ns) {
  Decision d;
  if (!passes_filter(m.delay_ns)) {
    ++rejected_;
    d.action = Action::Reject;
    d.freq_ppm = freq_ppm_;
    return d;
  }
  ++accepted_;

  if (accepted_ == 1 || std::llabs(m.offset_ns) > max_step_ns_) {
    d.action = Action::Step;
    d.phase_ns = m.offset_ns;
    d.freq_ppm = freq_ppm_;
    return d;
  }

  d.action = Action::Slew;
  d.phase_ns = static_cast<nanos_t>(std::llround(kp_ * static_cast<double>(m.offset_ns)));
  if (interval_ns > 0) {
    // Offset accumulated over `interval` implies a frequency error of offset/interval.
    const double observed_ppm = static_cast<double>(m.offset_ns) / static_cast<double>(interval_ns) * 1e6;
    freq_ppm_ = std::clamp(freq_ppm_ + ki_ * observed_ppm, -kMaxFreqPpm, kMaxFreqPpm);
  }
  d.freq_ppm = freq_ppm_;
  return d;
}

// --- NtpSynchronizer -----------------------------------------------------------

NtpSynchronizer::NtpSynchronizer(NtpConfig cfg, DisciplinedClock& clock)
    : cfg_(std::move(cfg)), clock_(clock), discipline_(cfg_) {}

NtpSynchronizer::~NtpSynchronizer() { stop(); }

void NtpSynchronizer::start() {
  if (running_.exchange(true)) return;
  rt::ThreadSpec spec{"cf-ntp", -1, 0, false, 64 * 1024};
  thread_ = rt::spawn(spec, [this] { loop(); });
}

void NtpSynchronizer::stop() {
  if (!running_.exchange(false)) return;
  if (thread_.joinable()) thread_.join();
}

NtpSynchronizer::Status NtpSynchronizer::status() const {
  Status s;
  s.last_offset_ns = last_offset_ns_.load(std::memory_order_relaxed);
  s.last_delay_ns = last_delay_ns_.load(std::memory_order_relaxed);
  s.freq_ppm = clock_.freq_ppm();
  s.accepted = accepted_.load(std::memory_order_relaxed);
  s.rejected = rejected_.load(std::memory_order_relaxed);
  s.failures = failures_.load(std::memory_order_relaxed);
  return s;
}

void NtpSynchronizer::poll_once() {
  auto m = query(cfg_.server, cfg_.port, cfg_.timeout_ms, [this] { return clock_.now_wall_ns(); });
  if (!m) {
    failures_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  last_offset_ns_.store(m->offset_ns, std::memory_order_relaxed);
  last_delay_ns_.store(m->delay_ns, std::memory_order_relaxed);

  const nanos_t now = mono_now();
  const nanos_t interval = last_accept_mono_ == 0 ? 0 : now - last_accept_mono_;
  const auto d = discipline_.update(*m, interval);

  switch (d.action) {
    case ClockDiscipline::Action::Reject:
      rejected_.fetch_add(1, std::memory_order_relaxed);
      CF_LOG_DEBUG("ntp: rejected sample offset=%.3fms delay=%.3fms (filter)", ns_to_ms(m->offset_ns),
                   ns_to_ms(m->delay_ns));
      return;
    case ClockDiscipline::Action::Step:
      CF_LOG_INFO("ntp: step %+.3f ms (delay %.3f ms, stratum %u)", ns_to_ms(m->offset_ns),
                  ns_to_ms(m->delay_ns), m->stratum);
      break;
    case ClockDiscipline::Action::Slew:
      CF_LOG_DEBUG("ntp: slew offset=%+.3fus phase=%+.3fus freq=%+.3fppm", ns_to_us(m->offset_ns),
                   ns_to_us(d.phase_ns), d.freq_ppm);
      break;
  }

  clock_.adjust(now, d.phase_ns, d.freq_ppm);
  clock_.mark_synchronized(true);
  accepted_.fetch_add(1, std::memory_order_relaxed);
  last_accept_mono_ = now;
}

void NtpSynchronizer::loop() {
  // Burst a few quick polls at startup so the filter and frequency converge fast,
  // then settle into the configured poll interval.
  constexpr int kBurst = 4;
  int polls = 0;
  nanos_t next = mono_now();
  while (running_.load(std::memory_order_acquire)) {
    if (mono_now() >= next) {
      poll_once();
      ++polls;
      const nanos_t interval = polls < kBurst ? 2 * kNsPerSec
                                              : static_cast<nanos_t>(std::max(1, cfg_.poll_interval_s)) * kNsPerSec;
      next = mono_now() + interval;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

}  // namespace chronofeed::ntp
