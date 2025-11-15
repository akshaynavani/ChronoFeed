#include <gtest/gtest.h>

#include <string>

#include "chronofeed/payload.hpp"

using namespace chronofeed;

TEST(Payload, ParsesCsv) {
  SensorSample s;
  ASSERT_EQ(parse_payload("sensors/x", "temp-01,42,1700000000000000000,21.5", s), PayloadError::None);
  EXPECT_STREQ(s.sensor_id.data(), "temp-01");
  EXPECT_EQ(s.seq, 42u);
  EXPECT_EQ(s.source_ts_ns, 1700000000000000000LL);
  EXPECT_DOUBLE_EQ(s.value, 21.5);
}

TEST(Payload, ParsesJsonInAnyKeyOrder) {
  SensorSample s;
  ASSERT_EQ(parse_payload("sensors/x", R"({"value": -3.25, "ts": 99, "id": "p1", "seq": 7})", s),
            PayloadError::None);
  EXPECT_STREQ(s.sensor_id.data(), "p1");
  EXPECT_EQ(s.seq, 7u);
  EXPECT_EQ(s.source_ts_ns, 99);
  EXPECT_DOUBLE_EQ(s.value, -3.25);
}

TEST(Payload, JsonFallsBackToTopicLeafForId) {
  SensorSample s;
  ASSERT_EQ(parse_payload("plant/line3/vib-9", R"({"value":1.0})", s), PayloadError::None);
  EXPECT_STREQ(s.sensor_id.data(), "vib-9");
}

TEST(Payload, JsonIgnoresUnknownKeys) {
  SensorSample s;
  EXPECT_EQ(parse_payload("t", R"({"unit":"C","value":4})", s), PayloadError::None);
}

TEST(Payload, RejectsMalformedInput) {
  SensorSample s;
  EXPECT_EQ(parse_payload("t", "", s), PayloadError::Empty);
  EXPECT_EQ(parse_payload("t", "a,1,2", s), PayloadError::BadFormat);
  EXPECT_EQ(parse_payload("t", "a,1,2,3,4", s), PayloadError::BadFormat);
  EXPECT_EQ(parse_payload("t", "a,x,2,3", s), PayloadError::BadNumber);
  EXPECT_EQ(parse_payload("t", "a,-1,2,3", s), PayloadError::BadNumber);
  EXPECT_EQ(parse_payload("t", "a,1,2,nan", s), PayloadError::BadNumber);
  EXPECT_EQ(parse_payload("t", R"({"id":"a"})", s), PayloadError::BadFormat);
  EXPECT_EQ(parse_payload("t", R"({"value":"1.0"})", s), PayloadError::BadNumber);
  EXPECT_EQ(parse_payload("t", std::string(600, 'x'), s), PayloadError::TooLong);
}

TEST(Payload, TruncatesLongSensorIds) {
  SensorSample s;
  const std::string id(100, 'z');
  ASSERT_EQ(parse_payload("t", id + ",1,2,3", s), PayloadError::None);
  EXPECT_EQ(std::string(s.sensor_id.data()).size(), kSensorIdMax - 1);
}
