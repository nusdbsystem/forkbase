// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_SINGLETON_H_
#define USTORE_UTILS_SINGLETON_H_

#include <utility>

namespace ustore {
/// Thread-safe implementation for C++11 according to
//  http://stackoverflow.com/questions/2576022/efficient-thread-safe-singleton-in-c
template <typename T>
class Singleton {
 public:
  //static T* Instance() {
  //  if (data_ == nullptr) {
  //      static T data;
  //      data_ = &data;
  //  }
  //  return data_;
  //}

  template<typename... Args>
  static T* Instance(Args&&... args) {
    if (data_ == nullptr) {
        static T data{std::forward<Args>(args)...};
        data_ = &data;
    }
    return data_;
  }

 private:
  static T* data_;
};

template <typename T>
T* Singleton<T>::data_ = nullptr;


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
