#pragma once

#include <array>
#include <cstdint>

#include "chronofeed/time.hpp"

namespace chronofeed {

constexpr std::size_t kSensorIdMax = 32;

// Fixed-size, trivially copyable record that flows through the pipeline.
// No heap members, so it can be moved through lock-free rings with memcpy semantics.
struct SensorSample {
  std::array<char, kSensorIdMax> sensor_id{};  // NUL-terminated
  std::uint64_t seq = 0;

  double value = 0.0;
  bool valid = false;

  // Publisher wall-clock timestamp (ns since UNIX epoch), as carried in the payload.
  nanos_t source_ts_ns = 0;
  // Disciplined wall-clock time at socket receipt.
  nanos_t ingest_wall_ns = 0;

  // Monotonic timestamps for deadline accounting.
  nanos_t rx_mono_ns = 0;        // read off the socket
  nanos_t parsed_mono_ns = 0;    // ingest stage done
  nanos_t processed_mono_ns = 0; // process stage done
  nanos_t deadline_mono_ns = 0;  // rx_mono_ns + budget

  // Derived by the process stage.
  double filtered_value = 0.0;
  bool missed_deadline = false;
};

}  // namespace chronofeed
