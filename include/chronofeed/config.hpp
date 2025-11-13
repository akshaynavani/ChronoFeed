#pragma once

#include <cstdint>
#include <map>
#include <string>

#include "chronofeed/time.hpp"

namespace chronofeed {

struct MqttConfig {
  std::string host = "127.0.0.1";
  std::uint16_t port = 1883;
  std::string client_id = "chronofeed";
  std::string topic = "sensors/#";
  std::string username;
  std::string password;
  std::uint16_t keepalive_s = 30;
  int reconnect_backoff_ms = 500;
};

struct RtConfig {
  bool enable_fifo = true;     // request SCHED_FIFO (needs CAP_SYS_NICE / rtprio limit)
  bool lock_memory = true;     // mlockall(MCL_CURRENT | MCL_FUTURE)
  bool busy_poll = true;       // consumers spin instead of sleeping when rings are empty
  int idle_sleep_us = 20;      // back-off sleep when busy_poll is false or spin budget is spent
  int ingest_cpu = 1;          // -1 = do not pin
  int process_cpu = 2;
  int sink_cpu = 3;
  int ingest_priority = 80;
  int process_priority = 75;
  int sink_priority = 60;
};

struct NtpConfig {
  bool enabled = true;
  std::string server = "pool.ntp.org";
  std::uint16_t port = 123;
  int poll_interval_s = 16;
  int timeout_ms = 500;
  double kp = 0.7;    // proportional gain of the discipline loop
  double ki = 0.05;   // integral gain (frequency correction)
  nanos_t max_step_ns = 128 * kNsPerMs;  // offsets beyond this are stepped, not slewed
};

struct PipelineConfig {
  nanos_t deadline_ns = 5 * kNsPerMs;  // end-to-end budget from socket rx to sink
  std::size_t ring_capacity = 4096;
  double ema_alpha = 0.2;              // smoothing factor for the process stage filter
  double value_min = -1e9;             // range validation
  double value_max = 1e9;
  std::string sink_path = "-";         // "-" = stdout, otherwise CSV file path
  int report_interval_s = 5;
};

struct Config {
  MqttConfig mqtt;
  RtConfig rt;
  NtpConfig ntp;
  PipelineConfig pipeline;
  std::string log_level = "info";
};

// Loads a simple `key = value` file (sections via dotted keys, e.g. `mqtt.host`).
// Lines starting with '#' are comments. Environment variables override file values:
// CHRONOFEED_MQTT_HOST, CHRONOFEED_MQTT_PORT, CHRONOFEED_MQTT_USERNAME,
// CHRONOFEED_MQTT_PASSWORD, CHRONOFEED_NTP_SERVER, CHRONOFEED_DEADLINE_MS.
// Throws std::runtime_error on malformed input.
Config load_config(const std::string& path);

// Applies a parsed key/value map on top of defaults (exposed for tests).
Config config_from_map(const std::map<std::string, std::string>& kv);

void apply_env_overrides(Config& cfg);

}  // namespace chronofeed
