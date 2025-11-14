#include "chronofeed/pipeline.hpp"

#include <chrono>

#include "chronofeed/log.hpp"
#include "chronofeed/rt/thread.hpp"

namespace chronofeed {

namespace {
std::unique_ptr<WallClockSource> make_clock(const NtpConfig& ntp) {
  if (ntp.enabled) return std::make_unique<ntp::DisciplinedClock>();
  return std::make_unique<SystemWallClock>();
}
}  // namespace

Pipeline::Pipeline(Config cfg)
    : cfg_(std::move(cfg)),
      clock_(make_clock(cfg_.ntp)),
      monitor_(cfg_.pipeline.deadline_ns),
      ring_a_(cfg_.pipeline.ring_capacity),
      ring_b_(cfg_.pipeline.ring_capacity) {
  if (cfg_.ntp.enabled) {
    disciplined_ = static_cast<ntp::DisciplinedClock*>(clock_.get());
    ntp_ = std::make_unique<ntp::NtpSynchronizer>(cfg_.ntp, *disciplined_);
  }
  ingest_ = std::make_unique<IngestStage>(ring_a_, monitor_, *clock_);
  process_ = std::make_unique<ProcessStage>(ring_a_, ring_b_, monitor_, cfg_.pipeline, cfg_.rt);
  sink_ = std::make_unique<SinkStage>(ring_b_, monitor_, cfg_.pipeline, cfg_.rt);
  client_ = std::make_unique<mqtt::Client>(cfg_.mqtt, *ingest_);
}

Pipeline::~Pipeline() { stop(); }

void Pipeline::start() {
  if (running_.exchange(true)) return;

  if (cfg_.rt.lock_memory) rt::lock_process_memory();

  const bool fifo = cfg_.rt.enable_fifo;
  rt::ThreadSpec sink_spec{"cf-sink", cfg_.rt.sink_cpu, cfg_.rt.sink_priority, fifo};
  rt::ThreadSpec proc_spec{"cf-process", cfg_.rt.process_cpu, cfg_.rt.process_priority, fifo};
  rt::ThreadSpec ingest_spec{"cf-ingest", cfg_.rt.ingest_cpu, cfg_.rt.ingest_priority, fifo};
  rt::ThreadSpec report_spec{"cf-report", -1, 0, false, 64 * 1024};

  // NTP runs best-effort in the background; samples are stamped with the seeded
  // system time until it converges (transit stats wait for synchronisation).
  if (ntp_) ntp_->start();

  // Start downstream first so consumers are ready before data flows.
  sink_thread_ = rt::spawn(sink_spec, [this] { sink_->run(running_); });
  process_thread_ = rt::spawn(proc_spec, [this] { process_->run(running_); });
  ingest_thread_ = rt::spawn(ingest_spec, [this] { client_->run(); });
  reporter_thread_ = rt::spawn(report_spec, [this] { reporter_loop(); });

  CF_LOG_INFO("pipeline: started with %.3f ms end-to-end deadline, ring capacity %zu",
              ns_to_ms(cfg_.pipeline.deadline_ns), ring_a_.capacity());
}

void Pipeline::stop() {
  if (!running_.exchange(false)) return;

  // Stop the producer first so downstream stages can drain.
  client_->stop();
  if (ingest_thread_.joinable()) ingest_thread_.join();
  if (process_thread_.joinable()) process_thread_.join();
  if (sink_thread_.joinable()) sink_thread_.join();
  if (reporter_thread_.joinable()) reporter_thread_.join();
  if (ntp_) ntp_->stop();

  CF_LOG_INFO("pipeline: final %s", monitor_.format(monitor_.report()).c_str());
}

void Pipeline::reporter_loop() {
  const nanos_t interval = static_cast<nanos_t>(cfg_.pipeline.report_interval_s > 0 ? cfg_.pipeline.report_interval_s : 5) * kNsPerSec;
  nanos_t next = mono_now() + interval;
  while (running_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if (mono_now() < next) continue;
    next += interval;

    const auto r = monitor_.report();
    const auto& cs = client_->stats();
    CF_LOG_INFO("report: %s | mqtt connected=%d msgs=%llu reconnects=%llu | ring_a=%zu ring_b=%zu",
                monitor_.format(r).c_str(), client_->connected() ? 1 : 0,
                static_cast<unsigned long long>(cs.messages.load()),
                static_cast<unsigned long long>(cs.reconnects.load()), ring_a_.size_approx(),
                ring_b_.size_approx());
    if (ntp_) {
      const auto ns = ntp_->status();
      CF_LOG_INFO("report: ntp synced=%d offset=%+.3fms delay=%.3fms freq=%+.3fppm ok=%llu rej=%llu fail=%llu",
                  disciplined_->synchronized() ? 1 : 0, ns_to_ms(ns.last_offset_ns), ns_to_ms(ns.last_delay_ns),
                  ns.freq_ppm, static_cast<unsigned long long>(ns.accepted),
                  static_cast<unsigned long long>(ns.rejected), static_cast<unsigned long long>(ns.failures));
    }
    if (r.missed > 0) {
      CF_LOG_WARN("report: deadline guarantee violated for %llu samples (%.4f%%)",
                  static_cast<unsigned long long>(r.missed), r.miss_ratio() * 100.0);
    }
  }
}

}  // namespace chronofeed
