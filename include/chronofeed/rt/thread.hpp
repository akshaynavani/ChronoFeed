#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <thread>

// OS-specific real-time controls. Everything platform dependent lives behind this
// header so that porting to a POSIX RTOS only touches src/rt/thread.cpp.
namespace chronofeed::rt {

struct ThreadSpec {
  std::string name;   // <= 15 chars on Linux
  int cpu = -1;       // core to pin to; -1 = leave unpinned
  int priority = 0;   // SCHED_FIFO priority 1..99; 0 = keep SCHED_OTHER
  bool fifo = false;  // request SCHED_FIFO
  std::size_t stack_prefault_bytes = 256 * 1024;
};

struct ApplyResult {
  bool named = false;
  bool pinned = false;
  bool realtime = false;
};

// Applies name / affinity / scheduling policy to the calling thread.
// Failures (e.g. missing CAP_SYS_NICE) are logged and reported, never fatal.
ApplyResult apply_to_current_thread(const ThreadSpec& spec);

// Touches `bytes` of stack so page faults happen at startup, not on the hot path.
void prefault_stack(std::size_t bytes);

// mlockall(MCL_CURRENT | MCL_FUTURE). Returns false if not permitted.
bool lock_process_memory();

// Number of online CPUs.
int online_cpus();

// Starts a thread that applies `spec` to itself before running `body`.
std::thread spawn(const ThreadSpec& spec, std::function<void()> body);

// CPU relax hint for spin loops.
inline void cpu_relax() {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
  asm volatile("yield" ::: "memory");
#else
  std::this_thread::yield();
#endif
}

}  // namespace chronofeed::rt
