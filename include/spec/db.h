// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_DB_H_
#define USTORE_SPEC_DB_H_

#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "types/ucell.h"

namespace ustore {

// DB is deprecated from v0.2
class DB {
 public:
  /**
   * @brief Read the value which is the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Get(const Slice& key, const Slice& branch,
                        Value* value) = 0;
  /**
   * @brief Read the value of a version.
   *
   * @param key     Target key.
   * @param versin  Version to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::kOK for success)
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
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value& value,
                        const Slice& branch, Hash* version) = 0;
  /**
   * @brief Write a new value as the successor of a version.
   *
   * @param key         Target key.
   * @param pre_version Previous version refered to.
   * @param value       Value to write.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value& value,
                        const Hash& pre_version, Hash* version) = 0;
  /**
   * @brief Create a new branch which points to the head of a branch.
   *
   * @param key         Target key.
   * @param old_branch  Existing branch.
   * @param new_branch  New branch.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) = 0;
  /**
   * @brief Create a new branch which points to a existing version.
   *
   * @param key         Target key.
   * @param version     Existing version.
   * @param new_branch  New branch.
   * @return            Error code. (ErrorCode::kOK for success)
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
   * @return            Error code. (ErrorCode::kOK for success)
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
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          Hash* version) = 0;
  /**
   * @brief Merge target branch to a referring version.
   *
   * @param key         Target key.
   * @param tgt_branch  The target branch.
   * @param ref_version The referring version.
   * @param value       (Optional) use if cannot auto-resolve conflicts.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Hash& ref_version,
                          Hash* version) = 0;
  /**
   * @brief Merge two existing versions.
   *
   * @param key           Target key.
   * @param ref_version1  The first referring branch.
   * @param ref_version2  The second referring version.
   * @param value         (Optional) use if cannot auto-resolve conflicts.
   * @param version       Returned version.
   * @return              Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& value,
                          const Hash& ref_version1, const Hash& ref_version2,
                          Hash* version) = 0;
};

class DB2 : public DB {
 public:
  /**
   * @brief Read the value which is the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Get(const Slice& key, const Slice& branch,
                        UCell* meta) = 0;
  /**
   * @brief Read the value of a version.
   *
   * @param key     Target key.
   * @param versin  Version to read.
   * @param value   Returned value.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Get(const Slice& key, const Hash& version,
                        UCell* meta) = 0;
  /**
   * @brief Write a new value as the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to update.
   * @param value   Value to write.
   * @param version Returned version.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value2& value,
                        const Slice& branch, Hash* version) = 0;
  /**
   * @brief Write a new value as the successor of a version.
   *
   * @param key         Target key.
   * @param pre_version Previous version refered to.
   * @param value       Value to write.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value2& value,
                        const Hash& pre_version, Hash* version) = 0;
  /**
   * @brief Merge target branch to a referring branch.
   *
   * @param key         Target key.
   * @param tgt_branch  The target branch.
   * @param ref_branch  The referring branch.
   * @param value       (Optional) use if cannot auto-resolve conflicts.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value2& value,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          Hash* version) = 0;
  /**
   * @brief Merge target branch to a referring version.
   *
   * @param key         Target key.
   * @param tgt_branch  The target branch.
   * @param ref_version The referring version.
   * @param value       (Optional) use if cannot auto-resolve conflicts.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value2& value,
                          const Slice& tgt_branch, const Hash& ref_version,
                          Hash* version) = 0;
  /**
   * @brief Merge two existing versions.
   *
   * @param key           Target key.
   * @param ref_version1  The first referring branch.
   * @param ref_version2  The second referring version.
   * @param value         (Optional) use if cannot auto-resolve conflicts.
   * @param version       Returned version.
   * @return              Error code. (ErrorCode::kOK for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value2& value,
                          const Hash& ref_version1, const Hash& ref_version2,
                          Hash* version) = 0;
  /**
   * @brief Read a chunk from a chunk id.
   *
   * TODO(wangsh): remove key later, when chunk store is partitioned by hash
   * @param key     Target key.
   * @param versin  Version to read.
   * @param chunk   Returned chunk.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  virtual Chunk GetChunk(const Slice& key, const Hash& version) = 0;
};

}  // namespace ustore

#endif  // USTORE_SPEC_DB_H_

