#include <gtest/gtest.h>

#include "chronofeed/histogram.hpp"

using namespace chronofeed;

TEST(LatencyHistogram, EmptySnapshotIsZero) {
  LatencyHistogram h;
  const auto s = h.snapshot();
  EXPECT_EQ(s.count, 0u);
  EXPECT_EQ(s.p99_ns, 0);
}

TEST(LatencyHistogram, PercentilesFollowDistribution) {
  LatencyHistogram h;
  // 990 samples at ~100us, 10 samples at ~3ms.
  for (int i = 0; i < 990; ++i) h.record(100 * kNsPerUs + 1);
  for (int i = 0; i < 10; ++i) h.record(3 * kNsPerMs + 1);

  const auto s = h.snapshot();
  EXPECT_EQ(s.count, 1000u);
  EXPECT_EQ(s.p50_ns, 110 * kNsPerUs);   // upper edge of the 100-110us bucket
  EXPECT_EQ(s.p99_ns, 110 * kNsPerUs);
  EXPECT_EQ(s.p999_ns, 3010 * kNsPerUs);
  EXPECT_EQ(s.max_ns, 3 * kNsPerMs + 1);
  EXPECT_EQ(s.min_ns, 100 * kNsPerUs + 1);
}

TEST(LatencyHistogram, OverflowStillTracksMax) {
  LatencyHistogram h;
  h.record(50 * kNsPerMs);
  const auto s = h.snapshot();
  EXPECT_EQ(s.overflow, 1u);
  EXPECT_EQ(s.max_ns, 50 * kNsPerMs);
  EXPECT_EQ(h.percentile(1.0), 50 * kNsPerMs);
}

TEST(LatencyHistogram, ResetClearsState) {
  LatencyHistogram h;
  h.record(1000);
  h.reset();
  EXPECT_EQ(h.snapshot().count, 0u);
}
