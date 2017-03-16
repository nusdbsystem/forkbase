// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_WORKER_H_
#define USTORE_WORKER_WORKER_H_

#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "utils/noncopyable.h"

namespace ustore {

/* Management related type alias */
using ErrorCode = int;

/**
 * @brief Worker node management.
 */
class Worker : private Noncopyable {
 public:
  explicit Worker(int id);
  ~Worker();

  inline int id() const { return id_; }

  /**
   * @brief Read data.
   *
   * @return Value of data.
   */
  ErrorCode Get(const Slice& key, Value* val) const;

  /**
   * @brief Write data with its specified version and branch.
   *
   * @param ver Data version.
   * @param branch The operating branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Put(const Slice& key, const Slice& branch, const Hash& version);

  /**
   * @brief Create a new branch for the data.
   *
   * @param bash_branch The base branch.
   * @param new_branch The new branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Fork(const Slice& key, const Slice& base_branch,
                 const Slice& new_branch);

  /**
   * @brief Move data to another branch.
   *
   * @param from_branch The original branch.
   * @param to_branch The target branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Move(const Slice& key, const Sliceh& from_branch,
                 const Slice& to_branch);

  /**
   * @brief Merge two branches of the data.
   *
   * @param op_branch The operating branch.
   * @param ref_branch The referring branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Merge(const Slice& key, const Slice& op_branch,
                  const Slice& ref_branch, const Hash& ref_hash);

 private:
  const int id_;
  const HeadVersion head_ver_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_H
