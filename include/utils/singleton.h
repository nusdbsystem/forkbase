// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_SINGLETON_H_
#define USTORE_UTILS_SINGLETON_H_

namespace ustore {
/// Thread-safe implementation for C++11 according to
//  http://stackoverflow.com/questions/2576022/efficient-thread-safe-singleton-in-c
template <typename T>
class Singleton {
 public:
  static T* Instance() {
    static T data_;
    return &data_;
  }
};

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
