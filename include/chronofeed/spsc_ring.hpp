#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>

namespace chronofeed {

#ifdef __cpp_lib_hardware_interference_size
constexpr std::size_t kCacheLine = std::hardware_destructive_interference_size;
#else
constexpr std::size_t kCacheLine = 64;
#endif

// Bounded single-producer / single-consumer lock-free ring buffer.
// Capacity is rounded up to a power of two. Storage is allocated once at construction
// so push/pop never allocate. Head and tail live on separate cache lines to avoid
// false sharing between the producer and consumer cores.
template <typename T>
class SpscRing {
  static_assert(std::is_trivially_copyable_v<T>, "SpscRing requires trivially copyable T");

 public:
  explicit SpscRing(std::size_t min_capacity)
      : capacity_(round_up_pow2(min_capacity < 2 ? 2 : min_capacity)),
        mask_(capacity_ - 1),
        buffer_(new T[capacity_]) {}

  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;

  // Producer side. Returns false if full (caller decides drop policy).
  bool try_push(const T& item) {
    const std::size_t head = head_.load(std::memory_order_relaxed);
    if (head - cached_tail_ >= capacity_) {
      cached_tail_ = tail_.load(std::memory_order_acquire);
      if (head - cached_tail_ >= capacity_) return false;
    }
    buffer_[head & mask_] = item;
    head_.store(head + 1, std::memory_order_release);
    return true;
  }

  // Consumer side. Returns false if empty.
  bool try_pop(T& out) {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);
    if (tail == cached_head_) {
      cached_head_ = head_.load(std::memory_order_acquire);
      if (tail == cached_head_) return false;
    }
    out = buffer_[tail & mask_];
    tail_.store(tail + 1, std::memory_order_release);
    return true;
  }

  std::size_t size_approx() const {
    const std::size_t h = head_.load(std::memory_order_acquire);
    const std::size_t t = tail_.load(std::memory_order_acquire);
    return h - t;
  }

  bool empty() const { return size_approx() == 0; }
  std::size_t capacity() const { return capacity_; }

 private:
  static std::size_t round_up_pow2(std::size_t v) {
    std::size_t p = 1;
    while (p < v) p <<= 1;
    return p;
  }

  const std::size_t capacity_;
  const std::size_t mask_;
  std::unique_ptr<T[]> buffer_;

  alignas(kCacheLine) std::atomic<std::size_t> head_{0};
  std::size_t cached_tail_ = 0;  // producer-local
  alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
  std::size_t cached_head_ = 0;  // consumer-local
};

}  // namespace chronofeed
