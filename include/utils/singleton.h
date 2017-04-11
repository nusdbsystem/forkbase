// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_SINGLETON_H_
#define USTORE_UTILS_SINGLETON_H_

#include <utility>

#include "thread_model.h"

namespace ustore {
/// Thread-safe implementation for C++11 according to
//  http://stackoverflow.com/questions/2576022/efficient-thread-safe-singleton-in-c

template <typename T, 
      template<typename> class ThreadedPolicy = SingleThreaded>
class Singleton : private ThreadedPolicy<T> {
 public:
  static T* Instance() {
      static T* data = MakeSingleton();
      return data;
  }

  template<typename... Args> 
  static T* MakeSingleton(Args&&... args) {
    if (data_ == nullptr) {
        typename ThreadedPolicy<T>::Lock lock();
        // double check
        if (data_ == nullptr) {
            static T data{std::forward<Args>(args)...};
            data_ = &data;
        }
    }
    return data_;
  }

 private:
  static T* data_;
};

template <typename T, 
      template<typename> class ThreadedPolicy>
T* Singleton<T, ThreadedPolicy>::data_ = nullptr;

/// Thread Specific Singleton
/// Each thread will have its own data_ storage.
/*
template<typename T>
class TSingleton {
 public:
  static T* Instance() {
    static thread_local T data_;  // thread_local is not available in some
                                  // compilers
    return &data_;
  }
};
*/
}  // namespace ustore

#endif  // USTORE_UTILS_SINGLETON_H_
