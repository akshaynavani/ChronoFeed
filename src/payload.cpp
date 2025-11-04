#include "chronofeed/payload.hpp"

#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>

namespace chronofeed {

namespace {

constexpr std::size_t kMaxPayload = 512;

std::string_view topic_leaf(std::string_view topic) {
  const auto slash = topic.rfind('/');
  return slash == std::string_view::npos ? topic : topic.substr(slash + 1);
}

std::string_view trim(std::string_view s) {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t' || s.front() == '\r' || s.front() == '\n'))
    s.remove_prefix(1);
  while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r' || s.back() == '\n'))
    s.remove_suffix(1);
  return s;
}

// strto* need NUL-terminated input; copy the token into a small stack buffer.
bool to_int64(std::string_view s, long long& out) {
  char buf[32];
  s = trim(s);
  if (s.empty() || s.size() >= sizeof(buf)) return false;
  std::memcpy(buf, s.data(), s.size());
  buf[s.size()] = '\0';
  char* end = nullptr;
  errno = 0;
  out = std::strtoll(buf, &end, 10);
  return errno == 0 && end == buf + s.size();
}

bool to_double(std::string_view s, double& out) {
  char buf[64];
  s = trim(s);
  if (s.empty() || s.size() >= sizeof(buf)) return false;
  std::memcpy(buf, s.data(), s.size());
  buf[s.size()] = '\0';
  char* end = nullptr;
  errno = 0;
  out = std::strtod(buf, &end);
  return errno == 0 && end == buf + s.size() && std::isfinite(out);
}

PayloadError parse_csv(std::string_view p, SensorSample& out) {
  std::string_view fields[4];
  std::size_t n = 0;
  while (n < 4) {
    const auto comma = p.find(',');
    if (comma == std::string_view::npos) {
      fields[n++] = p;
      break;
    }
    fields[n++] = p.substr(0, comma);
    p.remove_prefix(comma + 1);
    if (n == 4) return PayloadError::BadFormat;  // too many fields
  }
  if (n != 4) return PayloadError::BadFormat;

  const std::string_view id = trim(fields[0]);
  if (id.empty()) return PayloadError::BadFormat;
  set_sensor_id(out, id);

  long long seq = 0, ts = 0;
  double value = 0.0;
  if (!to_int64(fields[1], seq) || seq < 0) return PayloadError::BadNumber;
  if (!to_int64(fields[2], ts)) return PayloadError::BadNumber;
  if (!to_double(fields[3], value)) return PayloadError::BadNumber;

  out.seq = static_cast<std::uint64_t>(seq);
  out.source_ts_ns = ts;
  out.value = value;
  return PayloadError::None;
}

// Tiny flat-object JSON scanner: string and number values only, no nesting.
PayloadError parse_json(std::string_view topic, std::string_view p, SensorSample& out) {
  if (p.size() < 2 || p.front() != '{' || p.back() != '}') return PayloadError::BadFormat;
  p = p.substr(1, p.size() - 2);

  bool have_id = false, have_seq = false, have_ts = false, have_value = false;

  while (true) {
    p = trim(p);
    if (p.empty()) break;
    if (p.front() != '"') return PayloadError::BadFormat;
    const auto key_end = p.find('"', 1);
    if (key_end == std::string_view::npos) return PayloadError::BadFormat;
    const std::string_view key = p.substr(1, key_end - 1);
    p.remove_prefix(key_end + 1);
    p = trim(p);
    if (p.empty() || p.front() != ':') return PayloadError::BadFormat;
    p.remove_prefix(1);
    p = trim(p);

    std::string_view value;
    bool is_string = false;
    if (!p.empty() && p.front() == '"') {
      const auto vend = p.find('"', 1);
      if (vend == std::string_view::npos) return PayloadError::BadFormat;
      value = p.substr(1, vend - 1);
      is_string = true;
      p.remove_prefix(vend + 1);
    } else {
      const auto vend = p.find(',');
      value = trim(vend == std::string_view::npos ? p : p.substr(0, vend));
      p.remove_prefix(vend == std::string_view::npos ? p.size() : vend);
    }

    if (key == "id") {
      if (!is_string || value.empty()) return PayloadError::BadFormat;
      set_sensor_id(out, value);
      have_id = true;
    } else if (key == "seq") {
      long long v = 0;
      if (is_string || !to_int64(value, v) || v < 0) return PayloadError::BadNumber;
      out.seq = static_cast<std::uint64_t>(v);
      have_seq = true;
    } else if (key == "ts") {
      long long v = 0;
      if (is_string || !to_int64(value, v)) return PayloadError::BadNumber;
      out.source_ts_ns = v;
      have_ts = true;
    } else if (key == "value") {
      double v = 0;
      if (is_string || !to_double(value, v)) return PayloadError::BadNumber;
      out.value = v;
      have_value = true;
    }
    // Unknown keys are ignored for forward compatibility.

    p = trim(p);
    if (p.empty()) break;
    if (p.front() != ',') return PayloadError::BadFormat;
    p.remove_prefix(1);
  }

  if (!have_value) return PayloadError::BadFormat;
  if (!have_id) {
    const auto leaf = topic_leaf(topic);
    if (leaf.empty()) return PayloadError::BadFormat;
    set_sensor_id(out, leaf);
  }
  if (!have_seq) out.seq = 0;
  if (!have_ts) out.source_ts_ns = 0;
  return PayloadError::None;
}

}  // namespace

const char* payload_error_str(PayloadError e) {
  switch (e) {
    case PayloadError::None: return "ok";
    case PayloadError::Empty: return "empty payload";
    case PayloadError::TooLong: return "payload too long";
    case PayloadError::BadFormat: return "bad format";
    case PayloadError::BadNumber: return "bad number";
  }
  return "unknown";
}

void set_sensor_id(SensorSample& s, std::string_view id) {
  const std::size_t n = id.size() < kSensorIdMax - 1 ? id.size() : kSensorIdMax - 1;
  std::memcpy(s.sensor_id.data(), id.data(), n);
  s.sensor_id[n] = '\0';
}

PayloadError parse_payload(std::string_view topic, std::string_view payload, SensorSample& out) {
  payload = trim(payload);
  if (payload.empty()) return PayloadError::Empty;
  if (payload.size() > kMaxPayload) return PayloadError::TooLong;
  if (payload.front() == '{') return parse_json(topic, payload, out);
  return parse_csv(payload, out);
}

}  // namespace chronofeed
