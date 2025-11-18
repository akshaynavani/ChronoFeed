#include <gtest/gtest.h>

#include <cstdlib>
#include <stdexcept>

#include "chronofeed/config.hpp"

using namespace chronofeed;

TEST(Config, DefaultsGiveFiveMillisecondDeadline) {
  const Config c = config_from_map({});
  EXPECT_EQ(c.pipeline.deadline_ns, 5 * kNsPerMs);
  EXPECT_EQ(c.mqtt.port, 1883);
}

TEST(Config, AppliesKnownKeys) {
  const Config c = config_from_map({{"mqtt.host", "broker.local"},
                                    {"mqtt.port", "8883"},
                                    {"rt.process_cpu", "5"},
                                    {"rt.enable_fifo", "false"},
                                    {"pipeline.deadline_us", "2500"},
                                    {"ntp.kp", "0.5"}});
  EXPECT_EQ(c.mqtt.host, "broker.local");
  EXPECT_EQ(c.mqtt.port, 8883);
  EXPECT_EQ(c.rt.process_cpu, 5);
  EXPECT_FALSE(c.rt.enable_fifo);
  EXPECT_EQ(c.pipeline.deadline_ns, 2500 * kNsPerUs);
  EXPECT_DOUBLE_EQ(c.ntp.kp, 0.5);
}

TEST(Config, RejectsUnknownKeysAndBadValues) {
  EXPECT_THROW(config_from_map({{"mqtt.hots", "x"}}), std::runtime_error);
  EXPECT_THROW(config_from_map({{"mqtt.port", "abc"}}), std::runtime_error);
  EXPECT_THROW(config_from_map({{"rt.lock_memory", "maybe"}}), std::runtime_error);
  EXPECT_THROW(config_from_map({{"pipeline.deadline_us", "0"}}), std::runtime_error);
  EXPECT_THROW(config_from_map({{"pipeline.ema_alpha", "1.5"}}), std::runtime_error);
}

TEST(Config, EnvironmentOverridesCredentialsAndDeadline) {
  setenv("CHRONOFEED_MQTT_PASSWORD", "s3cret", 1);
  setenv("CHRONOFEED_DEADLINE_MS", "2", 1);
  Config c = config_from_map({});
  apply_env_overrides(c);
  EXPECT_EQ(c.mqtt.password, "s3cret");
  EXPECT_EQ(c.pipeline.deadline_ns, 2 * kNsPerMs);
  unsetenv("CHRONOFEED_MQTT_PASSWORD");
  unsetenv("CHRONOFEED_DEADLINE_MS");
}
