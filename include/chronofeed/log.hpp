#pragma once

#include <cstdarg>

namespace chronofeed::log {

enum class Level { Debug = 0, Info = 1, Warn = 2, Error = 3 };

void set_level(Level level);
Level level();
Level parse_level(const char* name);

// printf-style logging to stderr. Not intended for the hot path: stages only log
// from slow paths (connect, reconnect, periodic reports), never per sample.
void write(Level level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));

}  // namespace chronofeed::log

#define CF_LOG_DEBUG(...) ::chronofeed::log::write(::chronofeed::log::Level::Debug, __VA_ARGS__)
#define CF_LOG_INFO(...) ::chronofeed::log::write(::chronofeed::log::Level::Info, __VA_ARGS__)
#define CF_LOG_WARN(...) ::chronofeed::log::write(::chronofeed::log::Level::Warn, __VA_ARGS__)
#define CF_LOG_ERROR(...) ::chronofeed::log::write(::chronofeed::log::Level::Error, __VA_ARGS__)
