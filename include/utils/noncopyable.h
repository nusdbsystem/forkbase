// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NONCOPYABLE_H_
#define USTORE_NONCOPYABLE_H_

namespace ustore {
struct Noncopyable {
    Noncopyable() {}
    ~Noncopyable() {}
    Noncopyable(const Noncopyable&) = delete;
    Noncopyable& operator=(const Noncopyable&) = delete;
};
}
#endif
