#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

// Minimal MQTT 3.1.1 packet codec: just what a QoS 0 subscriber needs.
namespace chronofeed::mqtt {

enum class PacketType : std::uint8_t {
  Connect = 1,
  Connack = 2,
  Publish = 3,
  Puback = 4,
  Subscribe = 8,
  Suback = 9,
  Unsubscribe = 10,
  Unsuback = 11,
  Pingreq = 12,
  Pingresp = 13,
  Disconnect = 14,
};

struct ConnectOptions {
  std::string client_id;
  std::string username;
  std::string password;
  std::uint16_t keepalive_s = 30;
  bool clean_session = true;
};

// --- Encoders -------------------------------------------------------------

// Encodes the MQTT "remaining length" variable-byte integer (1-4 bytes).
// Returns false if the value exceeds the protocol maximum (268,435,455).
bool encode_remaining_length(std::size_t len, std::vector<std::uint8_t>& out);

std::vector<std::uint8_t> encode_connect(const ConnectOptions& opts);
std::vector<std::uint8_t> encode_subscribe(std::uint16_t packet_id, const std::string& topic,
                                           std::uint8_t qos);
std::vector<std::uint8_t> encode_publish(const std::string& topic, std::string_view payload);
std::vector<std::uint8_t> encode_puback(std::uint16_t packet_id);
std::vector<std::uint8_t> encode_pingreq();
std::vector<std::uint8_t> encode_disconnect();

// --- Decoder --------------------------------------------------------------

// A parsed fixed header plus a view of the variable header + payload.
struct RawPacket {
  PacketType type{};
  std::uint8_t flags = 0;
  const std::uint8_t* body = nullptr;
  std::size_t body_len = 0;
  std::size_t total_len = 0;  // bytes consumed from the stream
};

enum class DecodeStatus { Ok, NeedMore, Malformed };

// Attempts to frame one packet at the start of `data`. Never allocates.
DecodeStatus frame_packet(const std::uint8_t* data, std::size_t len, RawPacket& out);

// Non-owning view of a PUBLISH packet. Pointers are into the RawPacket body.
struct PublishView {
  std::string_view topic;
  std::string_view payload;
  std::uint8_t qos = 0;
  bool retain = false;
  bool dup = false;
  std::uint16_t packet_id = 0;  // only meaningful if qos > 0
};

bool parse_publish(const RawPacket& pkt, PublishView& out);

struct Connack {
  bool session_present = false;
  std::uint8_t return_code = 0;  // 0 = accepted
};

bool parse_connack(const RawPacket& pkt, Connack& out);

struct Suback {
  std::uint16_t packet_id = 0;
  std::uint8_t granted_qos = 0x80;  // 0x80 = failure
};

bool parse_suback(const RawPacket& pkt, Suback& out);

const char* connack_reason(std::uint8_t code);

}  // namespace chronofeed::mqtt
