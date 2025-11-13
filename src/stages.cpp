#include "chronofeed/stages.hpp"

#include <cstring>
#include <stdexcept>

#include "chronofeed/log.hpp"
#include "chronofeed/payload.hpp"
#include "chronofeed/rt/thread.hpp"

namespace chronofeed {

// --- IdleStrategy ------------------------------------------------------------

void IdleStrategy::idle() {
  constexpr std::uint32_t kSpinBudget = 4096;
  if (busy_ || spins_ < kSpinBudget) {
    ++spins_;
    rt::cpu_relax();
    return;
  }
  const nanos_t sleep_ns = static_cast<nanos_t>(sleep_us_ > 0 ? sleep_us_ : 1) * kNsPerUs;
  sleep_until_mono(mono_now() + sleep_ns);
}

// --- IngestStage -------------------------------------------------------------

IngestStage::IngestStage(SampleRing& out, DeadlineMonitor& monitor, const WallClockSource& clock)
    : out_(out), monitor_(monitor), clock_(clock) {}

void IngestStage::on_publish(const mqtt::PublishView& msg, nanos_t rx_mono_ns) {
  SensorSample s;
  monitor_.arm(s, rx_mono_ns);
  s.ingest_wall_ns = clock_.now_wall_ns();

  const PayloadError err = parse_payload(msg.topic, msg.payload, s);
  if (err != PayloadError::None) {
    monitor_.count_parse_error();
    return;
  }

  // Publisher -> us transit time is only meaningful when both clocks are disciplined.
  if (s.source_ts_ns > 0 && clock_.synchronized()) {
    monitor_.record_transit(s.ingest_wall_ns - s.source_ts_ns);
  }

  const nanos_t now = mono_now();
  s.parsed_mono_ns = now;
  monitor_.checkpoint(Stage::Ingest, s, rx_mono_ns, now);

  // Never block the socket reader: if the process stage has fallen behind, shed load.
  if (!out_.try_push(s)) monitor_.count_drop();
}

// --- SensorTable -------------------------------------------------------------

SensorTable::Entry* SensorTable::find_or_insert(const std::array<char, kSensorIdMax>& id) {
  // FNV-1a over the NUL-terminated id.
  std::uint64_t h = 1469598103934665603ULL;
  for (char c : id) {
    if (c == '\0') break;
    h ^= static_cast<unsigned char>(c);
    h *= 1099511628211ULL;
  }

  std::size_t idx = static_cast<std::size_t>(h) & (kCapacity - 1);
  for (std::size_t probe = 0; probe < kCapacity; ++probe) {
    Entry& e = entries_[idx];
    if (!e.used) {
      e.used = true;
      e.id = id;
      ++size_;
      return &e;
    }
    if (std::strncmp(e.id.data(), id.data(), kSensorIdMax) == 0) return &e;
    idx = (idx + 1) & (kCapacity - 1);
  }
  return nullptr;
}

// --- ProcessStage ------------------------------------------------------------

ProcessStage::ProcessStage(SampleRing& in, SampleRing& out, DeadlineMonitor& monitor,
                           const PipelineConfig& cfg, const RtConfig& rt)
    : in_(in),
      out_(out),
      monitor_(monitor),
      alpha_(cfg.ema_alpha),
      vmin_(cfg.value_min),
      vmax_(cfg.value_max),
      idle_(rt.busy_poll, rt.idle_sleep_us) {}

void ProcessStage::process(SensorSample& s) {
  s.valid = s.value >= vmin_ && s.value <= vmax_;
  if (!s.valid) {
    monitor_.count_invalid();
    s.filtered_value = s.value;
    return;
  }

  SensorTable::Entry* e = table_.find_or_insert(s.sensor_id);
  if (e == nullptr) {
    // Table full: pass through unfiltered rather than stall.
    s.filtered_value = s.value;
    return;
  }

  if (!e->primed) {
    e->ema = s.value;
    e->primed = true;
    e->last_seq = s.seq;
  } else {
    if (s.seq > e->last_seq + 1) monitor_.count_seq_gap(s.seq - e->last_seq - 1);
    e->ema = alpha_ * s.value + (1.0 - alpha_) * e->ema;
    if (s.seq > e->last_seq) e->last_seq = s.seq;
  }
  s.filtered_value = e->ema;
}

void ProcessStage::run(const std::atomic<bool>& running) {
  SensorSample s;
  while (running.load(std::memory_order_acquire)) {
    if (!in_.try_pop(s)) {
      idle_.idle();
      continue;
    }
    idle_.reset();
    const nanos_t start = mono_now();
    process(s);
    const nanos_t now = mono_now();
    s.processed_mono_ns = now;
    monitor_.checkpoint(Stage::Process, s, start, now);
    if (!out_.try_push(s)) monitor_.count_drop();
  }
}

// --- SinkStage ---------------------------------------------------------------

SinkStage::SinkStage(SampleRing& in, DeadlineMonitor& monitor, const PipelineConfig& cfg, const RtConfig& rt)
    : in_(in), monitor_(monitor), idle_(rt.busy_poll, rt.idle_sleep_us) {
  if (cfg.sink_path.empty() || cfg.sink_path == "-") {
    out_ = stdout;
  } else {
    out_ = std::fopen(cfg.sink_path.c_str(), "w");
    if (out_ == nullptr) throw std::runtime_error("sink: cannot open " + cfg.sink_path);
    owns_file_ = true;
  }
  std::setvbuf(out_, io_buf_.data(), _IOFBF, io_buf_.size());
  std::fprintf(out_, "sensor_id,seq,source_ts_ns,ingest_wall_ns,value,filtered,valid,e2e_us,missed\n");
}

SinkStage::~SinkStage() {
  if (out_ != nullptr) {
    std::fflush(out_);
    if (owns_file_) std::fclose(out_);
  }
}

void SinkStage::emit(const SensorSample& s) {
  const nanos_t now = mono_now();
  // Close out accounting *before* the write so the deadline measures pipeline latency,
  // not stdio throughput.
  SensorSample done = s;
  monitor_.checkpoint(Stage::Sink, done, s.processed_mono_ns, now);
  monitor_.complete(done, now);

  std::fprintf(out_, "%s,%llu,%lld,%lld,%.6f,%.6f,%d,%.1f,%d\n", done.sensor_id.data(),
               static_cast<unsigned long long>(done.seq), static_cast<long long>(done.source_ts_ns),
               static_cast<long long>(done.ingest_wall_ns), done.value, done.filtered_value,
               done.valid ? 1 : 0, ns_to_us(now - done.rx_mono_ns), done.missed_deadline ? 1 : 0);
}

void SinkStage::run(const std::atomic<bool>& running) {
  SensorSample s;
  nanos_t last_flush = mono_now();
  constexpr nanos_t kFlushEvery = 100 * kNsPerMs;

  while (running.load(std::memory_order_acquire)) {
    if (in_.try_pop(s)) {
      idle_.reset();
      emit(s);
    } else {
      idle_.idle();
    }
    const nanos_t now = mono_now();
    if (now - last_flush > kFlushEvery) {
      std::fflush(out_);
      last_flush = now;
    }
  }
  // Drain anything still queued at shutdown.
  while (in_.try_pop(s)) emit(s);
  std::fflush(out_);
}

}  // namespace chronofeed
