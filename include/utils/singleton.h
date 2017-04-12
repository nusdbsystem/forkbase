// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_SINGLETON_H_
#define USTORE_UTILS_SINGLETON_H_

#include <utility>

#include "thread_model.h"

namespace ustore {

// For thread safety, define USTORE_MULTI_THREADING and pass ClassLevelLockable 
// as the template template parameter
template <typename T, 
      template<typename> class ThreadedPolicy = SingleThreaded>
class Singleton : private ThreadedPolicy<T> {
 public:
  static T* Instance() {
      return data_ == nullptr ? MakeSingleton() : data_;
  }

  template<typename... Args> 
  static T* MakeSingleton(Args&&... args) {
      typename ThreadedPolicy<T>::Lock lock();
      // double check
      if (data_ == nullptr) {
          static T data{std::forward<Args>(args)...};
          data_ = &data;
      }
      return data_;
  }

 private:
  static T* data_;
};

template <typename T, 
      template<typename> class ThreadedPolicy>
T* Singleton<T, ThreadedPolicy>::data_ = nullptr;
}  // namespace ustore

#endif  // USTORE_UTILS_SINGLETON_H_
