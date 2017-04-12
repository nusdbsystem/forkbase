// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_THREAD_MODEL_H_
#define USTORE_UTILS_THREAD_MODEL_H_

#ifdef USTORE_MULTI_THREADING
#include <mutex>
#endif

#include "noncopyable.h"

namespace ustore {

template <typename T>
struct SingleThreaded {
    protected:
        class Lock : private Noncopyable {
            Lock () {}
            ~Lock () {}
        };
};

#ifdef USTORE_MULTI_THREADING
template <typename T>
struct ClassLevelLockable {
    protected:
        class Lock;
        friend class Lock;
        class Lock : private Noncopyable{
            // should be noncopyable; otherwise deadlock occurs in copt ctor
            // and assignment operator
            Lock () {
                ClassLevelLockable::mtx_.lock();
            }

            ~Lock() {
                ClassLevelLockable::mtx_.unlock();
            }
        };
    private:
        static std::mutex mtx_;
};

template <typename T>
std::mutex ClassLevelLockable<T>::mtx_;
#endif
};
#endif 
