#include "chronofeed/histogram.hpp"

#include <cmath>

namespace chronofeed {

nanos_t LatencyHistogram::percentile(double q) const {
  if (q < 0.0) q = 0.0;
  if (q > 1.0) q = 1.0;
  const std::uint64_t total = count_.load(std::memory_order_relaxed);
  if (total == 0) return 0;
  const std::uint64_t rank = static_cast<std::uint64_t>(std::ceil(q * static_cast<double>(total)));
  const std::uint64_t target = rank == 0 ? 1 : rank;

  std::uint64_t seen = 0;
  for (std::size_t i = 0; i < kBuckets; ++i) {
    seen += buckets_[i].load(std::memory_order_relaxed);
    if (seen >= target) return static_cast<nanos_t>(i + 1) * kBucketWidthNs;
  }
  // Falls in the overflow bucket: best we can say is the observed max.
  return max_.load(std::memory_order_relaxed);
}

LatencyHistogram::Snapshot LatencyHistogram::snapshot() const {
  Snapshot s;
  s.count = count_.load(std::memory_order_relaxed);
  if (s.count == 0) return s;
  s.min_ns = min_.load(std::memory_order_relaxed);
  s.max_ns = max_.load(std::memory_order_relaxed);
  s.mean_ns = static_cast<double>(sum_.load(std::memory_order_relaxed)) / static_cast<double>(s.count);
  s.p50_ns = percentile(0.50);
  s.p99_ns = percentile(0.99);
  s.p999_ns = percentile(0.999);
  s.overflow = overflow_.load(std::memory_order_relaxed);
  return s;
}

void LatencyHistogram::reset() {
  for (auto& b : buckets_) b.store(0, std::memory_order_relaxed);
  overflow_.store(0, std::memory_order_relaxed);
  count_.store(0, std::memory_order_relaxed);
  sum_.store(0, std::memory_order_relaxed);
  max_.store(0, std::memory_order_relaxed);
  min_.store(INT64_MAX, std::memory_order_relaxed);
}

}  // namespace chronofeed
