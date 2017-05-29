// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_USTORE_HTTP_SETTINGS_H_
#define USTORE_USTORE_HTTP_SETTINGS_H_

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
#define DEFAULT_CACHE_SIZE 100  // default cache size

#define MAX_HEADER_SIZE 512  // max http header size

/*
 * max file size
 * if actual file size > MAX_FILE_SIZE,
 * it is ok since we have a dynamic strategy to decide the file size
 * and allocate the response buffer
 */
#define MAX_FILE_SIZE 10240
#define MAX_RESPONSE_SIZE (MAX_HEADER_SIZE+MAX_FILE_SIZE)

/*
 * default receive buffer size
 * TODO: if data is larger than the DEFAULT_RECV_SIZE, how to handle?
 */
#define DEFAULT_RECV_SIZE MAX_RESPONSE_SIZE

/*
 * LOGGING LEVEL:
 *  LOG_DEBUG
 *  LOG_INFO
 *  LOG_WARNING
 *  LOG_FATAL
 */
#define LOG_LEVEL LOG_WARNING

}  // namespace ustore

#endif  // USTORE_USTORE_HTTP_SETTINGS_H_
