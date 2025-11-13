#include "chronofeed/config.hpp"

#include <cstdlib>
#include <fstream>
#include <functional>
#include <stdexcept>

namespace chronofeed {

namespace {

std::string trim(const std::string& s) {
  const auto b = s.find_first_not_of(" \t\r\n");
  if (b == std::string::npos) return {};
  const auto e = s.find_last_not_of(" \t\r\n");
  return s.substr(b, e - b + 1);
}

bool parse_bool(const std::string& key, const std::string& v) {
  if (v == "true" || v == "1" || v == "yes" || v == "on") return true;
  if (v == "false" || v == "0" || v == "no" || v == "off") return false;
  throw std::runtime_error("config: invalid boolean for " + key + ": " + v);
}

long long parse_int(const std::string& key, const std::string& v) {
  try {
    std::size_t pos = 0;
    long long out = std::stoll(v, &pos);
    if (pos != v.size()) throw std::invalid_argument("trailing");
    return out;
  } catch (const std::exception&) {
    throw std::runtime_error("config: invalid integer for " + key + ": " + v);
  }
}

double parse_double(const std::string& key, const std::string& v) {
  try {
    std::size_t pos = 0;
    double out = std::stod(v, &pos);
    if (pos != v.size()) throw std::invalid_argument("trailing");
    return out;
  } catch (const std::exception&) {
    throw std::runtime_error("config: invalid number for " + key + ": " + v);
  }
}

}  // namespace

Config config_from_map(const std::map<std::string, std::string>& kv) {
  Config c;
  using Setter = std::function<void(const std::string&, const std::string&)>;
  const std::map<std::string, Setter> setters = {
      {"log_level", [&](auto&, auto& v) { c.log_level = v; }},

      {"mqtt.host", [&](auto&, auto& v) { c.mqtt.host = v; }},
      {"mqtt.port", [&](auto& k, auto& v) { c.mqtt.port = static_cast<std::uint16_t>(parse_int(k, v)); }},
      {"mqtt.client_id", [&](auto&, auto& v) { c.mqtt.client_id = v; }},
      {"mqtt.topic", [&](auto&, auto& v) { c.mqtt.topic = v; }},
      {"mqtt.username", [&](auto&, auto& v) { c.mqtt.username = v; }},
      {"mqtt.keepalive_s", [&](auto& k, auto& v) { c.mqtt.keepalive_s = static_cast<std::uint16_t>(parse_int(k, v)); }},
      {"mqtt.reconnect_backoff_ms", [&](auto& k, auto& v) { c.mqtt.reconnect_backoff_ms = static_cast<int>(parse_int(k, v)); }},

      {"rt.enable_fifo", [&](auto& k, auto& v) { c.rt.enable_fifo = parse_bool(k, v); }},
      {"rt.lock_memory", [&](auto& k, auto& v) { c.rt.lock_memory = parse_bool(k, v); }},
      {"rt.busy_poll", [&](auto& k, auto& v) { c.rt.busy_poll = parse_bool(k, v); }},
      {"rt.idle_sleep_us", [&](auto& k, auto& v) { c.rt.idle_sleep_us = static_cast<int>(parse_int(k, v)); }},
      {"rt.ingest_cpu", [&](auto& k, auto& v) { c.rt.ingest_cpu = static_cast<int>(parse_int(k, v)); }},
      {"rt.process_cpu", [&](auto& k, auto& v) { c.rt.process_cpu = static_cast<int>(parse_int(k, v)); }},
      {"rt.sink_cpu", [&](auto& k, auto& v) { c.rt.sink_cpu = static_cast<int>(parse_int(k, v)); }},
      {"rt.ingest_priority", [&](auto& k, auto& v) { c.rt.ingest_priority = static_cast<int>(parse_int(k, v)); }},
      {"rt.process_priority", [&](auto& k, auto& v) { c.rt.process_priority = static_cast<int>(parse_int(k, v)); }},
      {"rt.sink_priority", [&](auto& k, auto& v) { c.rt.sink_priority = static_cast<int>(parse_int(k, v)); }},

      {"ntp.enabled", [&](auto& k, auto& v) { c.ntp.enabled = parse_bool(k, v); }},
      {"ntp.server", [&](auto&, auto& v) { c.ntp.server = v; }},
      {"ntp.port", [&](auto& k, auto& v) { c.ntp.port = static_cast<std::uint16_t>(parse_int(k, v)); }},
      {"ntp.poll_interval_s", [&](auto& k, auto& v) { c.ntp.poll_interval_s = static_cast<int>(parse_int(k, v)); }},
      {"ntp.timeout_ms", [&](auto& k, auto& v) { c.ntp.timeout_ms = static_cast<int>(parse_int(k, v)); }},
      {"ntp.kp", [&](auto& k, auto& v) { c.ntp.kp = parse_double(k, v); }},
      {"ntp.ki", [&](auto& k, auto& v) { c.ntp.ki = parse_double(k, v); }},
      {"ntp.max_step_ms", [&](auto& k, auto& v) { c.ntp.max_step_ns = parse_int(k, v) * kNsPerMs; }},

      {"pipeline.deadline_us", [&](auto& k, auto& v) { c.pipeline.deadline_ns = parse_int(k, v) * kNsPerUs; }},
      {"pipeline.ring_capacity", [&](auto& k, auto& v) { c.pipeline.ring_capacity = static_cast<std::size_t>(parse_int(k, v)); }},
      {"pipeline.ema_alpha", [&](auto& k, auto& v) { c.pipeline.ema_alpha = parse_double(k, v); }},
      {"pipeline.value_min", [&](auto& k, auto& v) { c.pipeline.value_min = parse_double(k, v); }},
      {"pipeline.value_max", [&](auto& k, auto& v) { c.pipeline.value_max = parse_double(k, v); }},
      {"pipeline.sink_path", [&](auto&, auto& v) { c.pipeline.sink_path = v; }},
      {"pipeline.report_interval_s", [&](auto& k, auto& v) { c.pipeline.report_interval_s = static_cast<int>(parse_int(k, v)); }},
  };

  for (const auto& [key, value] : kv) {
    auto it = setters.find(key);
    if (it == setters.end()) throw std::runtime_error("config: unknown key: " + key);
    it->second(key, value);
  }

  if (c.pipeline.deadline_ns <= 0) throw std::runtime_error("config: deadline must be positive");
  if (c.pipeline.ema_alpha <= 0.0 || c.pipeline.ema_alpha > 1.0)
    throw std::runtime_error("config: pipeline.ema_alpha must be in (0, 1]");
  return c;
}

void apply_env_overrides(Config& cfg) {
  if (const char* v = std::getenv("CHRONOFEED_MQTT_HOST")) cfg.mqtt.host = v;
  if (const char* v = std::getenv("CHRONOFEED_MQTT_PORT")) cfg.mqtt.port = static_cast<std::uint16_t>(std::atoi(v));
  if (const char* v = std::getenv("CHRONOFEED_MQTT_USERNAME")) cfg.mqtt.username = v;
  if (const char* v = std::getenv("CHRONOFEED_MQTT_PASSWORD")) cfg.mqtt.password = v;
  if (const char* v = std::getenv("CHRONOFEED_NTP_SERVER")) cfg.ntp.server = v;
  if (const char* v = std::getenv("CHRONOFEED_DEADLINE_MS")) {
    const double ms = std::atof(v);
    if (ms > 0) cfg.pipeline.deadline_ns = static_cast<nanos_t>(ms * kNsPerMs);
  }
}

Config load_config(const std::string& path) {
  std::map<std::string, std::string> kv;
  if (!path.empty()) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("config: cannot open " + path);
    std::string line;
    int lineno = 0;
    while (std::getline(in, line)) {
      ++lineno;
      const std::string t = trim(line);
      if (t.empty() || t[0] == '#') continue;
      const auto eq = t.find('=');
      if (eq == std::string::npos)
        throw std::runtime_error("config: line " + std::to_string(lineno) + ": expected key = value");
      kv[trim(t.substr(0, eq))] = trim(t.substr(eq + 1));
    }
  }
  Config cfg = config_from_map(kv);
  apply_env_overrides(cfg);
  return cfg;
}

}  // namespace chronofeed
