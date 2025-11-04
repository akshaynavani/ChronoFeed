#pragma once

#include <string_view>

#include "chronofeed/sample.hpp"

namespace chronofeed {

enum class PayloadError {
  None,
  Empty,
  TooLong,
  BadFormat,
  BadNumber,
};

const char* payload_error_str(PayloadError e);

// Parses one sensor reading into `out` without heap allocation. Two wire formats are
// accepted:
//
//   CSV:   <sensor_id>,<seq>,<ts_ns>,<value>
//   JSON:  {"id":"temp-01","seq":42,"ts":1700000000000000000,"value":21.5}
//
// For JSON, keys may appear in any order; `id` is optional and falls back to the last
// segment of the MQTT topic (e.g. "sensors/temp-01" -> "temp-01"). `ts` is the
// publisher's wall clock in nanoseconds since the UNIX epoch.
PayloadError parse_payload(std::string_view topic, std::string_view payload, SensorSample& out);

// Copies `id` into the fixed-size sample field, truncating if necessary.
void set_sensor_id(SensorSample& s, std::string_view id);

}  // namespace chronofeed
