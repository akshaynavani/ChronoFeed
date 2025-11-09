#include "chronofeed/rt/thread.hpp"

#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "chronofeed/log.hpp"

namespace chronofeed::rt {

void prefault_stack(std::size_t bytes) {
  if (bytes == 0) return;
  // volatile so the compiler cannot elide the writes.
  volatile unsigned char* buf = static_cast<volatile unsigned char*>(__builtin_alloca(bytes));
  const long page = sysconf(_SC_PAGESIZE) > 0 ? sysconf(_SC_PAGESIZE) : 4096;
  for (std::size_t i = 0; i < bytes; i += static_cast<std::size_t>(page)) buf[i] = 0;
}

bool lock_process_memory() {
  if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
    CF_LOG_WARN("rt: mlockall failed (%s); page faults may cause latency spikes", std::strerror(errno));
    return false;
  }
  return true;
}

int online_cpus() {
  const long n = sysconf(_SC_NPROCESSORS_ONLN);
  return n > 0 ? static_cast<int>(n) : 1;
}

ApplyResult apply_to_current_thread(const ThreadSpec& spec) {
  ApplyResult r;
  const pthread_t self = pthread_self();

#if defined(__linux__)
  if (!spec.name.empty()) {
    r.named = pthread_setname_np(self, spec.name.substr(0, 15).c_str()) == 0;
  }

  if (spec.cpu >= 0) {
    if (spec.cpu >= online_cpus()) {
      CF_LOG_WARN("rt: [%s] cpu %d not online (have %d); leaving unpinned", spec.name.c_str(),
                  spec.cpu, online_cpus());
    } else {
      cpu_set_t set;
      CPU_ZERO(&set);
      CPU_SET(spec.cpu, &set);
      const int rc = pthread_setaffinity_np(self, sizeof(set), &set);
      r.pinned = rc == 0;
      if (rc != 0)
        CF_LOG_WARN("rt: [%s] affinity to cpu %d failed: %s", spec.name.c_str(), spec.cpu, std::strerror(rc));
    }
  }
#elif defined(__APPLE__)
  if (!spec.name.empty()) r.named = pthread_setname_np(spec.name.c_str()) == 0;
  if (spec.cpu >= 0)
    CF_LOG_WARN("rt: [%s] hard CPU affinity is not supported on this platform", spec.name.c_str());
#endif

  if (spec.fifo && spec.priority > 0) {
    sched_param sp{};
    const int max_prio = sched_get_priority_max(SCHED_FIFO);
    const int min_prio = sched_get_priority_min(SCHED_FIFO);
    sp.sched_priority = spec.priority < min_prio ? min_prio : (spec.priority > max_prio ? max_prio : spec.priority);
    const int rc = pthread_setschedparam(self, SCHED_FIFO, &sp);
    r.realtime = rc == 0;
    if (rc != 0) {
      CF_LOG_WARN("rt: [%s] SCHED_FIFO prio %d denied (%s); running best-effort. "
                  "Grant CAP_SYS_NICE or raise RLIMIT_RTPRIO.",
                  spec.name.c_str(), sp.sched_priority, std::strerror(rc));
    }
  }

  prefault_stack(spec.stack_prefault_bytes);

  CF_LOG_INFO("rt: [%s] cpu=%d%s prio=%d%s", spec.name.c_str(), spec.cpu, r.pinned ? "" : " (unpinned)",
              spec.priority, r.realtime ? " FIFO" : " (best-effort)");
  return r;
}

std::thread spawn(const ThreadSpec& spec, std::function<void()> body) {
  return std::thread([spec, body = std::move(body)]() {
    apply_to_current_thread(spec);
    body();
  });
}

}  // namespace chronofeed::rt
