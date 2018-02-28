// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_HTTP_SETTINGS_H_
#define USTORE_HTTP_SETTINGS_H_

namespace ustore {

/*
 * whether use atomic or mutex
 * if defined, use atomic
 */
#define USE_ATOMIC

/*
 * whether use cache or not
 * if defined, use cache
 */
#define USE_CACHE
constexpr int kDefaultCacheSize = 100;  // default cache size

constexpr int kMaxHeaderSize = 10240;  // max http header size

/*
 * max file size
 * if actual file size > kMaxFileSize,
 * it is ok since we have a dynamic strategy to decide the file size
 * and allocate the response buffer
 */
constexpr int kMaxOutputSize = 1 << 30;  // max response message size
constexpr int kMaxResponseSize = kMaxHeaderSize + kMaxOutputSize;

/*
 * default receive buffer size
 * TODO: if data is larger than the kDefaultRecvSize, how to handle?
 */
constexpr int kMaxInputSize = 1 << 20;  // max input message size
constexpr int kDefaultRecvSize = kMaxHeaderSize + kMaxInputSize;

}  // namespace ustore

#endif  // USTORE_HTTP_SETTINGS_H_
