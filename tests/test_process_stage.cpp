#include <gtest/gtest.h>

#include "chronofeed/payload.hpp"
#include "chronofeed/stages.hpp"

using namespace chronofeed;

namespace {

SensorSample make(const char* id, std::uint64_t seq, double value) {
  SensorSample s;
  set_sensor_id(s, id);
  s.seq = seq;
  s.value = value;
  return s;
}

struct Fixture {
  PipelineConfig pcfg;
  RtConfig rcfg;
  DeadlineMonitor monitor{5 * kNsPerMs};
  SampleRing in{8};
  SampleRing out{8};
  ProcessStage stage;

  Fixture() : pcfg(make_cfg()), stage(in, out, monitor, pcfg, rcfg) {}

  static PipelineConfig make_cfg() {
    PipelineConfig c;
    c.ema_alpha = 0.5;
    c.value_min = -100.0;
    c.value_max = 100.0;
    return c;
  }
};

}  // namespace

TEST(ProcessStage, AppliesPerSensorExponentialMovingAverage) {
  Fixture f;
  auto a1 = make("a", 1, 10.0);
  auto b1 = make("b", 1, 50.0);
  auto a2 = make("a", 2, 20.0);
  f.stage.process(a1);
  f.stage.process(b1);
  f.stage.process(a2);
  EXPECT_DOUBLE_EQ(a1.filtered_value, 10.0);
  EXPECT_DOUBLE_EQ(b1.filtered_value, 50.0);
  EXPECT_DOUBLE_EQ(a2.filtered_value, 15.0);  // 0.5 * 20 + 0.5 * 10
}

TEST(ProcessStage, FlagsOutOfRangeValues) {
  Fixture f;
  auto s = make("a", 1, 1000.0);
  f.stage.process(s);
  EXPECT_FALSE(s.valid);
  EXPECT_EQ(f.monitor.report().invalid, 1u);
}

TEST(ProcessStage, CountsSequenceGaps) {
  Fixture f;
  auto s1 = make("a", 1, 1.0);
  auto s5 = make("a", 5, 1.0);
  f.stage.process(s1);
  f.stage.process(s5);
  EXPECT_EQ(f.monitor.report().seq_gaps, 3u);
}

TEST(SensorTable, ReturnsSameEntryForSameId) {
  SensorTable t;
  SensorSample a = make("x", 0, 0);
  auto* e1 = t.find_or_insert(a.sensor_id);
  auto* e2 = t.find_or_insert(a.sensor_id);
  EXPECT_EQ(e1, e2);
  EXPECT_EQ(t.size(), 1u);
}
