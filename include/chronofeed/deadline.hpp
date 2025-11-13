#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "chronofeed/histogram.hpp"
#include "chronofeed/sample.hpp"
#include "chronofeed/time.hpp"

namespace chronofeed {

enum class Stage : int { Ingest = 0, Process = 1, Sink = 2 };
constexpr int kStageCount = 3;

const char* stage_name(Stage s);

// Central accounting for the end-to-end deadline guarantee. Each stage reports its own
// latency and checks the sample's remaining budget. A miss is attributed to the first
// stage that observed the sample past its deadline, so the report shows *where* the
// budget was blown, not just that it was.
class DeadlineMonitor {
 public:
  explicit DeadlineMonitor(nanos_t budget_ns) : budget_ns_(budget_ns) {}

  nanos_t budget_ns() const { return budget_ns_; }

  // Stamps rx time and absolute deadline onto a freshly ingested sample.
  void arm(SensorSample& s, nanos_t rx_mono_ns) const {
    s.rx_mono_ns = rx_mono_ns;
    s.deadline_mono_ns = rx_mono_ns + budget_ns_;
    s.missed_deadline = false;
  }

  // Records the time spent in `stage` and checks the sample against its deadline.
  // Returns true if the sample is still within budget.
  bool checkpoint(Stage stage, SensorSample& s, nanos_t stage_start_ns, nanos_t now_ns) {
    stage_latency_[static_cast<int>(stage)].record(now_ns - stage_start_ns);
    if (now_ns > s.deadline_mono_ns && !s.missed_deadline) {
      s.missed_deadline = true;
      misses_by_stage_[static_cast<int>(stage)].fetch_add(1, std::memory_order_relaxed);
    }
    return !s.missed_deadline;
  }

  // Called once per sample when it leaves the pipeline.
  void complete(const SensorSample& s, nanos_t now_ns) {
    end_to_end_.record(now_ns - s.rx_mono_ns);
    completed_.fetch_add(1, std::memory_order_relaxed);
    if (s.missed_deadline) missed_.fetch_add(1, std::memory_order_relaxed);
  }

  void record_transit(nanos_t ns) { transit_.record(ns); }

  void count_drop() { dropped_.fetch_add(1, std::memory_order_relaxed); }
  void count_parse_error() { parse_errors_.fetch_add(1, std::memory_order_relaxed); }
  void count_invalid() { invalid_.fetch_add(1, std::memory_order_relaxed); }
  void count_seq_gap(std::uint64_t n) { seq_gaps_.fetch_add(n, std::memory_order_relaxed); }

  struct Report {
    nanos_t budget_ns = 0;
    std::uint64_t completed = 0;
    std::uint64_t missed = 0;
    std::uint64_t dropped = 0;
    std::uint64_t parse_errors = 0;
    std::uint64_t invalid = 0;
    std::uint64_t seq_gaps = 0;
    std::uint64_t misses_by_stage[kStageCount] = {};
    LatencyHistogram::Snapshot end_to_end;
    LatencyHistogram::Snapshot stage[kStageCount];
    LatencyHistogram::Snapshot transit;
    double miss_ratio() const {
      return completed == 0 ? 0.0 : static_cast<double>(missed) / static_cast<double>(completed);
    }
  };

  Report report() const;
  std::string format(const Report& r) const;

 private:
  const nanos_t budget_ns_;
  LatencyHistogram end_to_end_;
  LatencyHistogram stage_latency_[kStageCount];
  LatencyHistogram transit_;  // publisher wall clock -> disciplined ingest wall clock
  std::atomic<std::uint64_t> completed_{0};
  std::atomic<std::uint64_t> missed_{0};
  std::atomic<std::uint64_t> dropped_{0};
  std::atomic<std::uint64_t> parse_errors_{0};
  std::atomic<std::uint64_t> invalid_{0};
  std::atomic<std::uint64_t> seq_gaps_{0};
  std::atomic<std::uint64_t> misses_by_stage_[kStageCount] = {};
};

}  // namespace chronofeed
