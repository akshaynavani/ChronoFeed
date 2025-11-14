#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "chronofeed/clock_source.hpp"
#include "chronofeed/config.hpp"
#include "chronofeed/deadline.hpp"
#include "chronofeed/mqtt/client.hpp"
#include "chronofeed/ntp/discipline.hpp"
#include "chronofeed/stages.hpp"

namespace chronofeed {

// Owns every stage, ring and thread:
//
//   [mqtt/ingest thread] --ring A--> [process thread] --ring B--> [sink thread]
//            ^                                                        |
//     disciplined clock                                     DeadlineMonitor
//
// plus non-RT threads for NTP clock discipline and periodic deadline reports.
class Pipeline {
 public:
  explicit Pipeline(Config cfg);
  ~Pipeline();

  Pipeline(const Pipeline&) = delete;
  Pipeline& operator=(const Pipeline&) = delete;

  void start();
  void stop();  // idempotent; joins all threads

  const DeadlineMonitor& monitor() const { return monitor_; }
  DeadlineMonitor::Report final_report() const { return monitor_.report(); }

 private:
  void reporter_loop();

  Config cfg_;
  std::unique_ptr<WallClockSource> clock_;
  ntp::DisciplinedClock* disciplined_ = nullptr;  // non-owning view of clock_ when NTP is on
  std::unique_ptr<ntp::NtpSynchronizer> ntp_;
  DeadlineMonitor monitor_;
  SampleRing ring_a_;
  SampleRing ring_b_;
  std::unique_ptr<IngestStage> ingest_;
  std::unique_ptr<ProcessStage> process_;
  std::unique_ptr<SinkStage> sink_;
  std::unique_ptr<mqtt::Client> client_;

  std::atomic<bool> running_{false};
  std::thread ingest_thread_;
  std::thread process_thread_;
  std::thread sink_thread_;
  std::thread reporter_thread_;
};

}  // namespace chronofeed
