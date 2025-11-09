#pragma once

#include <array>
#include <atomic>
#include <cstdint>

#include "chronofeed/time.hpp"

namespace chronofeed {

// Fixed-bucket latency histogram. One writer thread records with relaxed atomics; any
// thread may take a snapshot. Buckets are 10 us wide up to 20 ms; anything above lands
// in the overflow bucket (and still updates max). No allocation, O(1) record.
class LatencyHistogram {
 public:
  static constexpr nanos_t kBucketWidthNs = 10 * kNsPerUs;
  static constexpr std::size_t kBuckets = 2000;  // covers [0, 20 ms)

  struct Snapshot {
    std::uint64_t count = 0;
    nanos_t min_ns = 0;
    nanos_t max_ns = 0;
    double mean_ns = 0.0;
    nanos_t p50_ns = 0;
    nanos_t p99_ns = 0;
    nanos_t p999_ns = 0;
    std::uint64_t overflow = 0;
  };

  void record(nanos_t ns) {
    if (ns < 0) ns = 0;
    const std::size_t idx = static_cast<std::size_t>(ns / kBucketWidthNs);
    if (idx < kBuckets) {
      buckets_[idx].fetch_add(1, std::memory_order_relaxed);
    } else {
      overflow_.fetch_add(1, std::memory_order_relaxed);
    }
    count_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(ns, std::memory_order_relaxed);
    nanos_t cur = max_.load(std::memory_order_relaxed);
    while (ns > cur && !max_.compare_exchange_weak(cur, ns, std::memory_order_relaxed)) {
    }
    cur = min_.load(std::memory_order_relaxed);
    while (ns < cur && !min_.compare_exchange_weak(cur, ns, std::memory_order_relaxed)) {
    }
  }

  Snapshot snapshot() const;

  // Percentile in [0, 1]; returns the upper edge of the bucket containing it.
  nanos_t percentile(double q) const;

  void reset();

 private:
  std::array<std::atomic<std::uint64_t>, kBuckets> buckets_{};
  std::atomic<std::uint64_t> overflow_{0};
  std::atomic<std::uint64_t> count_{0};
  std::atomic<nanos_t> sum_{0};
  std::atomic<nanos_t> max_{0};
  std::atomic<nanos_t> min_{INT64_MAX};
};

}  // namespace chronofeed
