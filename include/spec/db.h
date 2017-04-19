// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_DB_H_
#define USTORE_SPEC_DB_H_

#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/types.h"

namespace ustore {

class DB {
 public:
  /**
   * @brief Read the value which is the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Get(const Slice& key, const Slice& branch,
                        Value* value) = 0;
  /**
   * @brief Read the value of a version.
   *
   * @param key     Target key.
   * @param versin  Version to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Get(const Slice& key, const Hash& version,
                        Value* value) = 0;
  /**
   * @brief Write a new value as the head of a branch. 
   *
   * @param key     Target key.
   * @param branch  Branch to update.
   * @param value   Value to write.
   * @param version Returned version.
   * @return        Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Put(const Slice& key, const Slice& branch,
                        const Value& value, Hash* version) = 0;
  /**
   * @brief Write a new value as the successor of a version. 
   *
   * @param key         Target key.
   * @param pre_version Previous version refered to.
   * @param value       Value to write.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Put(const Slice& key, const Hash& pre_version,
                        const Value& value, Hash* version) = 0;
  /**
   * @brief Create a new branch which points to the head of a branch.
   *
   * @param key         Target key.
   * @param old_branch  Existing branch.
   * @param new_branch  New branch.
   * @return            Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) = 0;
  /**
   * @brief Create a new branch which points to a existing version.
   *
   * @param key         Target key.
   * @param version     Existing version.
   * @param new_branch  New branch.
   * @return            Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Hash& version,
                           const Slice& new_branch) = 0;
  /**
   * @brief Rename an existing branch. 
   *
   * @param key         Target key.
   * @param old_branch  Existing branch name.
   * @param new_branch  New branch name.
   *                    Remove the branch if new_branch = "".
   * @return            Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Rename(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) = 0;
  /**
   * @brief Merge target branch to a referring branch.
   *
   * @param key         Target key.
   * @param tgt_branch  The target branch.
   * @param ref_branch  The referring branch.
   * @param value       (Optional) use if cannot auto-resolve conflicts.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Slice& tgt_branch,
                          const Slice& ref_branch, const Value& value,
                          Hash* version) = 0;
  /**
   * @brief Merge target branch to a referring version.
   *
   * @param key         Target key.
   * @param tgt_branch  The target branch.
   * @param ref_version The referring version.
   * @param value       (Optional) use if cannot auto-resolve conflicts.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Slice& tgt_branch,
                          const Hash& ref_version, const Value& value,
                          Hash* version) = 0;
  /**
   * @brief Merge two existing versions. 
   *
   * @param key           Target key.
   * @param ref_version1  The first referring branch.
   * @param ref_version2  The second referring version.
   * @param value         (Optional) use if cannot auto-resolve conflicts.
   * @param version       Returned version.
   * @return              Error code. (ErrorCode::ok for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Hash& ref_version1,
                          const Hash& ref_version2, const Value& value,
                          Hash* version) = 0;
}

}  // namespace ustore

#endif  // USTORE_SPEC_DB_H_

