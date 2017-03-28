// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_WORKER_H_
#define USTORE_WORKER_WORKER_H_

#include "head_version.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

using WorkerID = uint32_t;

/**
 * @brief Worker node management.
 */
class Worker : private Noncopyable {
 public:
  static Worker* Instance();

  explicit Worker(const WorkerID& id);
  ~Worker();

  inline WorkerID id() const { return id_; }

  /**
   * @brief Read data.
   *
   * @param key Data key.
   * @param branch The operating branch.
   * @param version Data version.
   * @param val Accommodator of the to-be-retrieved value.
   * @return Error code. (0 for success)
   */
  ErrorCode Get(const Slice& key, const Slice& branch, const Hash& version,
                Value* val) const;

  /**
   * @brief Write data with its specified version and branch.
   *
   * @param key Data key.
   * @param val Data value.
   * @param branch The operating branch.
   * @param version Data version.
   * @return Error code. (0 for success)
   */
  ErrorCode Put(const Slice& key, const Value& val, const Slice& branch,
                const Hash& version);

  /**
   * @brief Create a new branch for the data.
   *
   * @param old_branch The base branch. If provided, the new branch should be
   *                   based on the head version.
   * @param version Data version.
   * @param new_branch The new branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                   const Hash& version, const Slice& new_branch);

  /**
   * @brief Move data to another branch.
   *
   * @param key Data key.
   * @param old_branch The original branch.
   * @param new_branch The target branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Move(const Slice& key,
                 const Slice& old_branch,
                 const Slice& new_branch);

  /**
   * @brief Merge two branches of the data.
   *
   * @param key Data key.
   * @param val Data value.
   * @param tgt_branch The target branch.
   * @param ref_branch The referring branch.
   * @param ref_version The referring version of data.
   * @return Error code. (0 for success)
   */
  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Slice& ref_branch, const Hash& ref_version);

 private:
  const WorkerID id_;
  const HeadVersion* head_ver_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_H
