// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPE_TRAITS_H_
#define USTORE_TYPE_TRAITS_H_

namespace ustore {
class noncopyable {
    protected:
    noncopyable() {}
    ~noncopyable() {}
    noncopyable(const noncopyable&) = delete;
    const noncopyable& operator=(const noncopyable&) = delete;
};
}
#endif
