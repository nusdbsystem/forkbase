// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_HEAD_VERSION_H_
#define USTORE_WORKER_HEAD_VERSION_H_

#include "hash/hash.h"
#include "spec/slice.h"
#include "utils/noncopyable.h"

namespace ustore {

/**
 * @brief Table of head versions of data.
 *
 * This class should only be instantiated by Worker.
 */
class HeadVersion : private Noncopyable {
 public:
  static HeadVersion* Instance();
  HeadVersion();
  ~HeadVersion();

  /**
   * @brief Retrieve the head version of data according to the specified
   *        branch.
   *
   * @param key Data key.
   * @param branch Branch to look for.
   * @return Head version of data as per request.
   */
  const Hash& Get(const Slice& key, const Slice& branch) const;

  /**
   * @brief Update the head version of data according to the specified
   *        branch.
   *
   * @param ver The updating version.
   * @param key Data key.
   * @param branch Branch to update.
   * @return Error code. (0 for success)
   */
  void Put(const Slice& key, const Slice& branch, const Hash& version);
};

}  // namespace ustore

#endif  // USTORE_WORKER_HEAD_VERSION_H
