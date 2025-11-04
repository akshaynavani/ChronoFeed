#include "chronofeed/mqtt/packet.hpp"

namespace chronofeed::mqtt {

namespace {

constexpr std::size_t kMaxRemainingLength = 268'435'455;

void put_u16(std::vector<std::uint8_t>& out, std::uint16_t v) {
  out.push_back(static_cast<std::uint8_t>(v >> 8));
  out.push_back(static_cast<std::uint8_t>(v & 0xFF));
}

void put_string(std::vector<std::uint8_t>& out, std::string_view s) {
  put_u16(out, static_cast<std::uint16_t>(s.size()));
  out.insert(out.end(), s.begin(), s.end());
}

std::uint16_t get_u16(const std::uint8_t* p) {
  return static_cast<std::uint16_t>((p[0] << 8) | p[1]);
}

std::vector<std::uint8_t> finish(std::uint8_t first_byte, const std::vector<std::uint8_t>& body) {
  std::vector<std::uint8_t> out;
  out.reserve(body.size() + 5);
  out.push_back(first_byte);
  encode_remaining_length(body.size(), out);
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

}  // namespace

bool encode_remaining_length(std::size_t len, std::vector<std::uint8_t>& out) {
  if (len > kMaxRemainingLength) return false;
  do {
    std::uint8_t byte = static_cast<std::uint8_t>(len % 128);
    len /= 128;
    if (len > 0) byte |= 0x80;
    out.push_back(byte);
  } while (len > 0);
  return true;
}

std::vector<std::uint8_t> encode_connect(const ConnectOptions& opts) {
  std::vector<std::uint8_t> body;
  put_string(body, "MQTT");
  body.push_back(0x04);  // protocol level 3.1.1

  std::uint8_t flags = 0;
  if (opts.clean_session) flags |= 0x02;
  if (!opts.username.empty()) flags |= 0x80;
  if (!opts.password.empty()) flags |= 0x40;
  body.push_back(flags);
  put_u16(body, opts.keepalive_s);

  put_string(body, opts.client_id);
  if (!opts.username.empty()) put_string(body, opts.username);
  if (!opts.password.empty()) put_string(body, opts.password);

  return finish(static_cast<std::uint8_t>(PacketType::Connect) << 4, body);
}

std::vector<std::uint8_t> encode_subscribe(std::uint16_t packet_id, const std::string& topic,
                                           std::uint8_t qos) {
  std::vector<std::uint8_t> body;
  put_u16(body, packet_id);
  put_string(body, topic);
  body.push_back(qos & 0x03);
  // SUBSCRIBE fixed header flags must be 0b0010.
  return finish(static_cast<std::uint8_t>((static_cast<int>(PacketType::Subscribe) << 4) | 0x02), body);
}

std::vector<std::uint8_t> encode_publish(const std::string& topic, std::string_view payload) {
  std::vector<std::uint8_t> body;
  body.reserve(topic.size() + payload.size() + 2);
  put_string(body, topic);
  body.insert(body.end(), payload.begin(), payload.end());
  return finish(static_cast<std::uint8_t>(PacketType::Publish) << 4, body);
}

std::vector<std::uint8_t> encode_puback(std::uint16_t packet_id) {
  std::vector<std::uint8_t> body;
  put_u16(body, packet_id);
  return finish(static_cast<std::uint8_t>(PacketType::Puback) << 4, body);
}

std::vector<std::uint8_t> encode_pingreq() {
  return {static_cast<std::uint8_t>(static_cast<int>(PacketType::Pingreq) << 4), 0x00};
}

std::vector<std::uint8_t> encode_disconnect() {
  return {static_cast<std::uint8_t>(static_cast<int>(PacketType::Disconnect) << 4), 0x00};
}

DecodeStatus frame_packet(const std::uint8_t* data, std::size_t len, RawPacket& out) {
  if (len < 2) return DecodeStatus::NeedMore;

  std::size_t remaining = 0;
  std::size_t multiplier = 1;
  std::size_t idx = 1;
  while (true) {
    if (idx > 4) return DecodeStatus::Malformed;  // more than 4 length bytes
    if (idx >= len) return DecodeStatus::NeedMore;
    const std::uint8_t byte = data[idx++];
    remaining += static_cast<std::size_t>(byte & 0x7F) * multiplier;
    if ((byte & 0x80) == 0) break;
    multiplier *= 128;
  }

  const std::uint8_t type = data[0] >> 4;
  if (type == 0 || type == 15) return DecodeStatus::Malformed;
  if (len - idx < remaining) return DecodeStatus::NeedMore;

  out.type = static_cast<PacketType>(type);
  out.flags = data[0] & 0x0F;
  out.body = data + idx;
  out.body_len = remaining;
  out.total_len = idx + remaining;
  return DecodeStatus::Ok;
}

bool parse_publish(const RawPacket& pkt, PublishView& out) {
  if (pkt.type != PacketType::Publish || pkt.body_len < 2) return false;
  out.dup = (pkt.flags & 0x08) != 0;
  out.qos = (pkt.flags >> 1) & 0x03;
  out.retain = (pkt.flags & 0x01) != 0;
  if (out.qos == 3) return false;

  const std::uint8_t* p = pkt.body;
  const std::uint8_t* end = pkt.body + pkt.body_len;
  const std::uint16_t topic_len = get_u16(p);
  p += 2;
  if (static_cast<std::size_t>(end - p) < topic_len) return false;
  out.topic = std::string_view(reinterpret_cast<const char*>(p), topic_len);
  p += topic_len;

  out.packet_id = 0;
  if (out.qos > 0) {
    if (end - p < 2) return false;
    out.packet_id = get_u16(p);
    p += 2;
  }
  out.payload = std::string_view(reinterpret_cast<const char*>(p), static_cast<std::size_t>(end - p));
  return true;
}

bool parse_connack(const RawPacket& pkt, Connack& out) {
  if (pkt.type != PacketType::Connack || pkt.body_len != 2) return false;
  out.session_present = (pkt.body[0] & 0x01) != 0;
  out.return_code = pkt.body[1];
  return true;
}

bool parse_suback(const RawPacket& pkt, Suback& out) {
  if (pkt.type != PacketType::Suback || pkt.body_len < 3) return false;
  out.packet_id = get_u16(pkt.body);
  out.granted_qos = pkt.body[2];
  return true;
}

const char* connack_reason(std::uint8_t code) {
  switch (code) {
    case 0: return "accepted";
    case 1: return "unacceptable protocol version";
    case 2: return "identifier rejected";
    case 3: return "server unavailable";
    case 4: return "bad username or password";
    case 5: return "not authorized";
    default: return "unknown";
  }
}

}  // namespace chronofeed::mqtt
