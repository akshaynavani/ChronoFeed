#include <gtest/gtest.h>

#include "chronofeed/deadline.hpp"

using namespace chronofeed;

TEST(DeadlineMonitor, ArmsAbsoluteDeadlineFromReceiveTime) {
  DeadlineMonitor m(5 * kNsPerMs);
  SensorSample s;
  m.arm(s, 1'000'000);
  EXPECT_EQ(s.rx_mono_ns, 1'000'000);
  EXPECT_EQ(s.deadline_mono_ns, 6'000'000);
  EXPECT_FALSE(s.missed_deadline);
}

TEST(DeadlineMonitor, OnTimeSampleIsNotAMiss) {
  DeadlineMonitor m(5 * kNsPerMs);
  SensorSample s;
  m.arm(s, 0);
  EXPECT_TRUE(m.checkpoint(Stage::Ingest, s, 0, 1 * kNsPerMs));
  EXPECT_TRUE(m.checkpoint(Stage::Process, s, 1 * kNsPerMs, 2 * kNsPerMs));
  EXPECT_TRUE(m.checkpoint(Stage::Sink, s, 2 * kNsPerMs, 5 * kNsPerMs));  // exactly on deadline
  m.complete(s, 5 * kNsPerMs);

  const auto r = m.report();
  EXPECT_EQ(r.completed, 1u);
  EXPECT_EQ(r.missed, 0u);
}

TEST(DeadlineMonitor, AttributesMissToFirstLateStage) {
  DeadlineMonitor m(5 * kNsPerMs);
  SensorSample s;
  m.arm(s, 0);
  EXPECT_TRUE(m.checkpoint(Stage::Ingest, s, 0, 1 * kNsPerMs));
  EXPECT_FALSE(m.checkpoint(Stage::Process, s, 1 * kNsPerMs, 6 * kNsPerMs));
  EXPECT_FALSE(m.checkpoint(Stage::Sink, s, 6 * kNsPerMs, 7 * kNsPerMs));
  m.complete(s, 7 * kNsPerMs);

  const auto r = m.report();
  EXPECT_EQ(r.missed, 1u);
  EXPECT_EQ(r.misses_by_stage[static_cast<int>(Stage::Ingest)], 0u);
  EXPECT_EQ(r.misses_by_stage[static_cast<int>(Stage::Process)], 1u);
  EXPECT_EQ(r.misses_by_stage[static_cast<int>(Stage::Sink)], 0u);
  EXPECT_DOUBLE_EQ(r.miss_ratio(), 1.0);
}

TEST(DeadlineMonitor, CountsDropsAndErrors) {
  DeadlineMonitor m(kNsPerMs);
  m.count_drop();
  m.count_parse_error();
  m.count_parse_error();
  m.count_invalid();
  m.count_seq_gap(3);
  const auto r = m.report();
  EXPECT_EQ(r.dropped, 1u);
  EXPECT_EQ(r.parse_errors, 2u);
  EXPECT_EQ(r.invalid, 1u);
  EXPECT_EQ(r.seq_gaps, 3u);
  EXPECT_FALSE(m.format(r).empty());
}
