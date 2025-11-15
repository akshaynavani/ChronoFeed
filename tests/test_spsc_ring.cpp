#include <gtest/gtest.h>

#include <cstdint>
#include <thread>

#include "chronofeed/spsc_ring.hpp"

using chronofeed::SpscRing;

TEST(SpscRing, RoundsCapacityUpToPowerOfTwo) {
  SpscRing<int> r(5);
  EXPECT_EQ(r.capacity(), 8u);
}

TEST(SpscRing, PushPopPreservesFifoOrder) {
  SpscRing<int> r(4);
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(r.try_push(i));
  for (int i = 0; i < 4; ++i) {
    int v = -1;
    ASSERT_TRUE(r.try_pop(v));
    EXPECT_EQ(v, i);
  }
  int v;
  EXPECT_FALSE(r.try_pop(v));
}

TEST(SpscRing, RejectsPushWhenFull) {
  SpscRing<int> r(2);
  EXPECT_TRUE(r.try_push(1));
  EXPECT_TRUE(r.try_push(2));
  EXPECT_FALSE(r.try_push(3));
  int v;
  ASSERT_TRUE(r.try_pop(v));
  EXPECT_TRUE(r.try_push(3));
}

TEST(SpscRing, WrapsAroundManyTimes) {
  SpscRing<int> r(4);
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(r.try_push(i));
    int v;
    ASSERT_TRUE(r.try_pop(v));
    ASSERT_EQ(v, i);
  }
  EXPECT_TRUE(r.empty());
}

TEST(SpscRing, ConcurrentProducerConsumerSeesEverySequenceInOrder) {
  constexpr std::uint64_t kCount = 500'000;
  SpscRing<std::uint64_t> r(1024);

  std::thread producer([&] {
    for (std::uint64_t i = 0; i < kCount;) {
      if (r.try_push(i)) ++i;
    }
  });

  std::uint64_t expected = 0;
  while (expected < kCount) {
    std::uint64_t v;
    if (r.try_pop(v)) {
      ASSERT_EQ(v, expected);
      ++expected;
    }
  }
  producer.join();
  EXPECT_TRUE(r.empty());
}
