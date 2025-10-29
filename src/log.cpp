#include "chronofeed/log.hpp"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <mutex>

namespace chronofeed::log {

namespace {
std::atomic<int> g_level{static_cast<int>(Level::Info)};
std::mutex g_mutex;

const char* tag(Level l) {
  switch (l) {
    case Level::Debug: return "DEBUG";
    case Level::Info: return "INFO ";
    case Level::Warn: return "WARN ";
    case Level::Error: return "ERROR";
  }
  return "?????";
}
}  // namespace

void set_level(Level level) { g_level.store(static_cast<int>(level), std::memory_order_relaxed); }

Level level() { return static_cast<Level>(g_level.load(std::memory_order_relaxed)); }

Level parse_level(const char* name) {
  if (name == nullptr) return Level::Info;
  if (std::strcmp(name, "debug") == 0) return Level::Debug;
  if (std::strcmp(name, "warn") == 0) return Level::Warn;
  if (std::strcmp(name, "error") == 0) return Level::Error;
  return Level::Info;
}

void write(Level lvl, const char* fmt, ...) {
  if (static_cast<int>(lvl) < g_level.load(std::memory_order_relaxed)) return;

  char msg[1024];
  va_list args;
  va_start(args, fmt);
  std::vsnprintf(msg, sizeof(msg), fmt, args);
  va_end(args);

  timespec ts{};
  clock_gettime(CLOCK_REALTIME, &ts);
  tm tm_utc{};
  gmtime_r(&ts.tv_sec, &tm_utc);
  char when[32];
  std::strftime(when, sizeof(when), "%H:%M:%S", &tm_utc);

  std::lock_guard<std::mutex> lock(g_mutex);
  std::fprintf(stderr, "%s.%06ld [%s] %s\n", when, ts.tv_nsec / 1000, tag(lvl), msg);
}

}  // namespace chronofeed::log
