#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "chronofeed/mqtt/packet.hpp"

using namespace chronofeed::mqtt;

TEST(MqttRemainingLength, EncodesBoundaryValues) {
  struct Case {
    std::size_t value;
    std::vector<std::uint8_t> bytes;
  };
  const Case cases[] = {
      {0, {0x00}},
      {127, {0x7F}},
      {128, {0x80, 0x01}},
      {16383, {0xFF, 0x7F}},
      {16384, {0x80, 0x80, 0x01}},
      {268435455, {0xFF, 0xFF, 0xFF, 0x7F}},
  };
  for (const auto& c : cases) {
    std::vector<std::uint8_t> out;
    ASSERT_TRUE(encode_remaining_length(c.value, out));
    EXPECT_EQ(out, c.bytes) << "value " << c.value;
  }
  std::vector<std::uint8_t> out;
  EXPECT_FALSE(encode_remaining_length(268435456, out));
}

TEST(MqttConnect, SetsFlagsForCredentials) {
  ConnectOptions o;
  o.client_id = "cf";
  o.username = "u";
  o.password = "p";
  o.keepalive_s = 30;
  const auto pkt = encode_connect(o);
  ASSERT_GE(pkt.size(), 12u);
  EXPECT_EQ(pkt[0], 0x10);
  // Variable header: 00 04 'M' 'Q' 'T' 'T' 04 flags 00 1E
  EXPECT_EQ(pkt[2], 0x00);
  EXPECT_EQ(pkt[3], 0x04);
  EXPECT_EQ(std::string(pkt.begin() + 4, pkt.begin() + 8), "MQTT");
  EXPECT_EQ(pkt[8], 0x04);
  EXPECT_EQ(pkt[9], 0x80 | 0x40 | 0x02);
  EXPECT_EQ(pkt[10], 0x00);
  EXPECT_EQ(pkt[11], 30);
}

TEST(MqttSubscribe, HasReservedFlagBits) {
  const auto pkt = encode_subscribe(7, "sensors/#", 0);
  EXPECT_EQ(pkt[0], 0x82);
  EXPECT_EQ(pkt[2], 0x00);
  EXPECT_EQ(pkt[3], 0x07);
  EXPECT_EQ(pkt.back(), 0x00);
}

TEST(MqttFrame, RoundTripsPublish) {
  const auto bytes = encode_publish("sensors/t1", "t1,1,100,2.5");
  RawPacket raw;
  ASSERT_EQ(frame_packet(bytes.data(), bytes.size(), raw), DecodeStatus::Ok);
  EXPECT_EQ(raw.type, PacketType::Publish);
  EXPECT_EQ(raw.total_len, bytes.size());

  PublishView view;
  ASSERT_TRUE(parse_publish(raw, view));
  EXPECT_EQ(view.topic, "sensors/t1");
  EXPECT_EQ(view.payload, "t1,1,100,2.5");
  EXPECT_EQ(view.qos, 0);
}

TEST(MqttFrame, ReportsNeedMoreOnPartialInput) {
  const auto bytes = encode_publish("a/b", "payload");
  RawPacket raw;
  for (std::size_t n = 0; n < bytes.size(); ++n) {
    EXPECT_EQ(frame_packet(bytes.data(), n, raw), DecodeStatus::NeedMore) << "prefix " << n;
  }
}

TEST(MqttFrame, FramesBackToBackPackets) {
  auto a = encode_publish("x", "1");
  const auto b = encode_pingreq();
  a.insert(a.end(), b.begin(), b.end());

  RawPacket first;
  ASSERT_EQ(frame_packet(a.data(), a.size(), first), DecodeStatus::Ok);
  RawPacket second;
  ASSERT_EQ(frame_packet(a.data() + first.total_len, a.size() - first.total_len, second), DecodeStatus::Ok);
  EXPECT_EQ(second.type, PacketType::Pingreq);
}

TEST(MqttFrame, RejectsOverlongRemainingLength) {
  const std::uint8_t bad[] = {0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
  RawPacket raw;
  EXPECT_EQ(frame_packet(bad, sizeof(bad), raw), DecodeStatus::Malformed);
}

TEST(MqttPublish, ParsesQos1PacketId) {
  // PUBLISH qos1: 0x32, len, topic "t", packet id 0x0102, payload "v"
  const std::uint8_t bytes[] = {0x32, 0x06, 0x00, 0x01, 't', 0x01, 0x02, 'v'};
  RawPacket raw;
  ASSERT_EQ(frame_packet(bytes, sizeof(bytes), raw), DecodeStatus::Ok);
  PublishView view;
  ASSERT_TRUE(parse_publish(raw, view));
  EXPECT_EQ(view.qos, 1);
  EXPECT_EQ(view.packet_id, 0x0102);
  EXPECT_EQ(view.payload, "v");
}

TEST(MqttConnack, ParsesReturnCode) {
  const std::uint8_t bytes[] = {0x20, 0x02, 0x00, 0x05};
  RawPacket raw;
  ASSERT_EQ(frame_packet(bytes, sizeof(bytes), raw), DecodeStatus::Ok);
  Connack ack;
  ASSERT_TRUE(parse_connack(raw, ack));
  EXPECT_EQ(ack.return_code, 5);
  EXPECT_STREQ(connack_reason(ack.return_code), "not authorized");
}
