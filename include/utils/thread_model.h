// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_THREAD_MODEL_H_
#define USTORE_UTILS_THREAD_MODEL_H_

#include <mutex>

#include "noncopyable.h"

template <typename T>
struct SingleThreaded {
    public:
        class Lock {
            Lock () {}
            ~Lock () {}
        };
};


template <typename T>
struct ClassLevelLockable {
    class Lock;
    friend class Lock;
    public:
        class Lock {
            Lock () {
                ClassLevelLockable::mtx_.lock();
            }

            ~Lock() {
                ClassLevelLockable::mtx_.unlock();
            }

            Lock (const Lock&) = delete;
            Lock& operator=(const Lock&) = delete;
        };
    private:
        static std::mutex mtx_;
};

template <typename T>
std::mutex ClassLevelLockable<T>::mtx_;
#endif 
