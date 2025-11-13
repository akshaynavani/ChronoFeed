#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <string>

#include "chronofeed/clock_source.hpp"
#include "chronofeed/config.hpp"
#include "chronofeed/deadline.hpp"
#include "chronofeed/mqtt/client.hpp"
#include "chronofeed/sample.hpp"
#include "chronofeed/spsc_ring.hpp"

namespace chronofeed {

using SampleRing = SpscRing<SensorSample>;

// Waits for work on an empty ring: spin with a CPU relax hint for a bounded number of
// iterations, then sleep briefly. With busy_poll the spin budget is unbounded, which
// trades a dedicated core for the lowest wake-up latency.
class IdleStrategy {
 public:
  IdleStrategy(bool busy_poll, int sleep_us) : busy_(busy_poll), sleep_us_(sleep_us) {}
  void idle();
  void reset() { spins_ = 0; }

 private:
  bool busy_;
  int sleep_us_;
  std::uint32_t spins_ = 0;
};

// Stage 1: runs on the MQTT client thread. Parses payloads into fixed-size samples,
// arms their deadline from the socket receive time, and hands them to the process stage.
class IngestStage final : public mqtt::PublishHandler {
 public:
  IngestStage(SampleRing& out, DeadlineMonitor& monitor, const WallClockSource& clock);
  void on_publish(const mqtt::PublishView& msg, nanos_t rx_mono_ns) override;

 private:
  SampleRing& out_;
  DeadlineMonitor& monitor_;
  const WallClockSource& clock_;
};

// Per-sensor state for filtering and sequence tracking. Open-addressed, fixed capacity,
// so no allocation on the hot path even when new sensors appear.
class SensorTable {
 public:
  static constexpr std::size_t kCapacity = 1024;

  struct Entry {
    std::array<char, kSensorIdMax> id{};
    bool used = false;
    bool primed = false;
    double ema = 0.0;
    std::uint64_t last_seq = 0;
  };

  // Returns nullptr when the table is full.
  Entry* find_or_insert(const std::array<char, kSensorIdMax>& id);
  std::size_t size() const { return size_; }

 private:
  std::array<Entry, kCapacity> entries_{};
  std::size_t size_ = 0;
};

// Stage 2: validates range, detects sequence gaps, applies a per-sensor exponential
// moving average, and forwards to the sink.
class ProcessStage {
 public:
  ProcessStage(SampleRing& in, SampleRing& out, DeadlineMonitor& monitor, const PipelineConfig& cfg,
               const RtConfig& rt);
  void run(const std::atomic<bool>& running);
  // Exposed for tests: processes one sample in place.
  void process(SensorSample& s);

 private:
  SampleRing& in_;
  SampleRing& out_;
  DeadlineMonitor& monitor_;
  double alpha_;
  double vmin_;
  double vmax_;
  IdleStrategy idle_;
  SensorTable table_;
};

// Stage 3: emits samples as CSV lines and closes out deadline accounting. Output goes
// through a large stdio buffer flushed on a timer, so disk/terminal stalls are absorbed
// by the buffer rather than the deadline path.
class SinkStage {
 public:
  SinkStage(SampleRing& in, DeadlineMonitor& monitor, const PipelineConfig& cfg, const RtConfig& rt);
  ~SinkStage();
  void run(const std::atomic<bool>& running);

 private:
  void emit(const SensorSample& s);

  SampleRing& in_;
  DeadlineMonitor& monitor_;
  IdleStrategy idle_;
  std::FILE* out_ = nullptr;
  bool owns_file_ = false;
  std::array<char, 1 << 20> io_buf_{};
};

}  // namespace chronofeed
