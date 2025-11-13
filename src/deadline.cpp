#include "chronofeed/deadline.hpp"

#include <cstdio>

namespace chronofeed {

const char* stage_name(Stage s) {
  switch (s) {
    case Stage::Ingest: return "ingest";
    case Stage::Process: return "process";
    case Stage::Sink: return "sink";
  }
  return "?";
}

DeadlineMonitor::Report DeadlineMonitor::report() const {
  Report r;
  r.budget_ns = budget_ns_;
  r.completed = completed_.load(std::memory_order_relaxed);
  r.missed = missed_.load(std::memory_order_relaxed);
  r.dropped = dropped_.load(std::memory_order_relaxed);
  r.parse_errors = parse_errors_.load(std::memory_order_relaxed);
  r.invalid = invalid_.load(std::memory_order_relaxed);
  r.seq_gaps = seq_gaps_.load(std::memory_order_relaxed);
  for (int i = 0; i < kStageCount; ++i) {
    r.misses_by_stage[i] = misses_by_stage_[i].load(std::memory_order_relaxed);
    r.stage[i] = stage_latency_[i].snapshot();
  }
  r.end_to_end = end_to_end_.snapshot();
  r.transit = transit_.snapshot();
  return r;
}

std::string DeadlineMonitor::format(const Report& r) const {
  char buf[1024];
  int n = std::snprintf(
      buf, sizeof(buf),
      "deadline=%.3fms done=%llu missed=%llu (%.4f%%) dropped=%llu parse_err=%llu invalid=%llu gaps=%llu | "
      "e2e us p50=%.1f p99=%.1f p99.9=%.1f max=%.1f",
      ns_to_ms(r.budget_ns), static_cast<unsigned long long>(r.completed),
      static_cast<unsigned long long>(r.missed), r.miss_ratio() * 100.0,
      static_cast<unsigned long long>(r.dropped), static_cast<unsigned long long>(r.parse_errors),
      static_cast<unsigned long long>(r.invalid), static_cast<unsigned long long>(r.seq_gaps),
      ns_to_us(r.end_to_end.p50_ns), ns_to_us(r.end_to_end.p99_ns), ns_to_us(r.end_to_end.p999_ns),
      ns_to_us(r.end_to_end.max_ns));

  for (int i = 0; i < kStageCount && n > 0 && static_cast<std::size_t>(n) < sizeof(buf); ++i) {
    n += std::snprintf(buf + n, sizeof(buf) - static_cast<std::size_t>(n),
                       " | %s p99=%.1fus max=%.1fus miss=%llu", stage_name(static_cast<Stage>(i)),
                       ns_to_us(r.stage[i].p99_ns), ns_to_us(r.stage[i].max_ns),
                       static_cast<unsigned long long>(r.misses_by_stage[i]));
  }
  if (r.transit.count > 0 && n > 0 && static_cast<std::size_t>(n) < sizeof(buf)) {
    std::snprintf(buf + n, sizeof(buf) - static_cast<std::size_t>(n), " | transit p50=%.2fms p99=%.2fms",
                  ns_to_ms(r.transit.p50_ns), ns_to_ms(r.transit.p99_ns));
  }
  return buf;
}

}  // namespace chronofeed
