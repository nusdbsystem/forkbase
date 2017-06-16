// Copyright (c) 2017 The UStore Authors.

#ifndef USTORE_UTILS_BLOCKING_QUEUE_H_
#define USTORE_UTILS_BLOCKING_QUEUE_H_

#include <condition_variable>
#include <list>
#include <mutex>
#include "utils/noncopyable.h"

namespace ustore {

template<typename T>
class BlockingQueue : private Noncopyable {
 public:
  BlockingQueue(size_t capacity) : capacity_(capacity) {}

  void Put(const T& x) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      while (queue_.size() == capacity_) cond_var_not_full_.wait(mutex_);
      queue_.emplace_back(x);
    }
    cond_var_not_empty_.notify_one();
  }

  void Put(const T&& x) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      while (queue_.size() == capacity_) cond_var_not_full_.wait(mutex_);
      queue_.push_back(x);
    }
    cond_var_not_empty_.notify_one();
  }

  T Take() {
    std::lock_guard<std::mutex> lock(mutex_);
    while (queue_.empty()) cond_var_not_empty_.wait(mutex_);
    auto x = queue_.front();
    queue_.pop_front();
    cond_var_not_full_.notify_one();
    return x;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
  }

  size_t capacity() const { return capacity_; }

 private:
  const size_t capacity_;
  std::list<T> queue_;
  std::mutex mutex_;
  std::condition_variable_any cond_var_not_empty_, cond_var_not_full_;
};

}  // namespace ustore

#endif  // USTORE_UTILS_BLOCKING_QUEUE_H_
