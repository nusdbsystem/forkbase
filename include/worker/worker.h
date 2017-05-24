// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_WORKER_H_
#define USTORE_WORKER_WORKER_H_

#include <unordered_set>
#include <vector>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "spec/db.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "store/chunk_store.h"
#include "types/type.h"
#include "types/ucell.h"
#include "utils/noncopyable.h"
#include "worker/head_version.h"

namespace ustore {

using WorkerID = uint32_t;

/**
 * @brief Worker node management.
 */
class Worker : public DB, private Noncopyable {
 public:
  explicit Worker(const WorkerID& id) : id_(id) {}
  ~Worker() {}

  inline WorkerID id() const { return id_; }

  /**
   * @brief Obtain the head version of the specified branch.
   *
   * @param key Data key,
   * @param branch The operating branch.
   * @return Head version of the branch; Hash::kNull if the requesting head
   *         version is unavailable.
   */
  inline Hash GetBranchHead(const Slice& key, const Slice& branch) const {
    const auto& ver_opt = head_ver_.GetBranch(key, branch);
    return ver_opt ? *ver_opt : Hash::kNull;
  }

  /**
   * @brief Update latest version by a UCell.
   *
   * @param key Data key.
   * @param ucell The referring UCell object.
   */
  inline void UpdateLatestVersion(const UCell& ucell) {
    const auto& prev_ver1 = ucell.preHash();
    const auto& prev_ver2 = ucell.preHash(true);
    const auto& ver = ucell.hash();
    head_ver_.PutLatest(ucell.key(), prev_ver1, prev_ver2, ver);
  }

  /**
   * @brief Read the value which is the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to read.
   * @param ucell   Accommodator of the to-be-retrieved UCell object.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  ErrorCode Get(const Slice& key, const Slice& branch, UCell* ucell) override;

  /**
   * @brief Read the value of a version.
   *
   * @param key     Target key.
   * @param ver     Version to read.
   * @param ucell   Accommodator of the to-be-retrieved UCell object.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  ErrorCode Get(const Slice& key, const Hash& ver, UCell* ucell) override;

  /**
   * @brief Write a new value as the head of a branch.
   *
   * @param key     Target key.
   * @param branch  Branch to update.
   * @param value   Value to write.
   * @param version Returned version.
   * @return        Error code. (ErrorCode::kOK for success)
   */
  inline ErrorCode Put(const Slice& key, const Value2& val, const Slice& branch,
                       Hash* ver) override {
    return Put(key, val, branch, GetBranchHead(key, branch), ver);
  }

  ErrorCode Put(const Slice& key, const Value2& val, const Slice& branch) {
    static Hash ver;
    return Put(key, val, branch, &ver);
  }

  /**
   * @brief Write a new value as the successor of a version.
   *
   * @param key         Target key.
   * @param pre_version Previous version refered to.
   * @param value       Value to write.
   * @param version     Returned version.
   * @return            Error code. (ErrorCode::kOK for success)
   */
  inline ErrorCode Put(const Slice& key, const Value2& val, const Hash& prev_ver,
                       Hash* ver) override {
    return Write(key, val, prev_ver, Hash::kNull, ver);
  }

  ErrorCode Put(const Slice& key, const Value2& val, const Hash& prev_ver) {
    static Hash ver;
    return Put(key, val, prev_ver, &ver);
  }

  /**
   * @brief Create a new branch for the data.
   *
   * @param old_branch The base branch.
   * @param new_branch The new branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch) override;

  /**
   * @brief Create a new branch for the data.
   *
   * @param ver Data version.
   * @param new_branch The new branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Branch(const Slice& key, const Hash& ver,
                   const Slice& new_branch) override;

  /**
   * @brief Rename the branch.
   *
   * @param key Data key.
   * @param old_branch The original branch.
   * @param new_branch The target branch.
   * @return Error code. (0 for success)
   */
  ErrorCode Rename(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch) override;

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
  inline ErrorCode Merge(const Slice& key, const Value2& val,
                         const Slice& tgt_branch, const Slice& ref_branch,
                         Hash* ver) override {
    return MergeImpl(key, val, tgt_branch, ref_branch, ver);
  }

  ErrorCode Merge(const Slice& key, const Value2& val,
                  const Slice& tgt_branch, const Slice& ref_branch) {
    static Hash ver;
    return MergeImpl(key, val, tgt_branch, ref_branch, &ver);
  }

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
  inline ErrorCode Merge(const Slice& key, const Value2& val,
                         const Slice& tgt_branch, const Hash& ref_ver,
                         Hash* ver) override {
    return MergeImpl(key, val, tgt_branch, ref_ver, ver);
  }

  ErrorCode Merge(const Slice& key, const Value2& val,
                  const Slice& tgt_branch, const Hash& ref_ver) {
    static Hash ver;
    return MergeImpl(key, val, tgt_branch, ref_ver, &ver);
  }

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
  inline ErrorCode Merge(const Slice& key, const Value2& val,
                         const Hash& ref_ver1, const Hash& ref_ver2,
                         Hash* ver) override {
    return MergeImpl(key, val, ref_ver1, ref_ver2, ver);
  }

  ErrorCode Merge(const Slice& key, const Value2& val,
                  const Hash& ref_ver1, const Hash& ref_ver2) {
    static Hash ver;
    return MergeImpl(key, val, ref_ver1, ref_ver2, &ver);
  }

  Chunk GetChunk(const Slice& key, const Hash& ver) override;

 protected:
  HeadVersion head_ver_;

 private:
  ErrorCode CreateUCell(const Slice& key, const UType& utype,
                        const Hash& utype_hash, const Hash& prev_ver1,
                        const Hash& prev_ver2, Hash* ver);
  ErrorCode Write(const Slice& key, const Value2& val, const Hash& prev_ver1,
                  const Hash& prev_ver2, Hash* ver);
  ErrorCode WriteBlob(const Slice& key, const Value2& val, const Hash& prev_ver1,
                      const Hash& prev_ver2, Hash* ver);
  ErrorCode WriteString(const Slice& key, const Value2& val,
                        const Hash& prev_ver1, const Hash& prev_ver2,
                        Hash* ver);
  ErrorCode WriteList(const Slice& key, const Value2& val,
                      const Hash& prev_ver1, const Hash& prev_ver2,
                      Hash* ver);
  ErrorCode WriteMap(const Slice& key, const Value2& val,
                     const Hash& prev_ver1, const Hash& prev_ver2,
                     Hash* ver);
  ErrorCode Put(const Slice& key, const Value2& val, const Slice& branch,
                const Hash& prev_ver, Hash* ver);

  template<class T>
  ErrorCode MergeImpl(const Slice& key, const T& val,
                      const Slice& tgt_branch, const Slice& ref_branch,
                      Hash* ver);
  template<class T>
  ErrorCode MergeImpl(const Slice& key, const T& val,
                      const Slice& tgt_branch, const Hash& ref_ver,
                      Hash* ver);
  template<class T>
  ErrorCode MergeImpl(const Slice& key, const T& val,
                      const Hash& ref_ver1, const Hash& ref_ver2,
                      Hash* ver);

  const WorkerID id_;
};

template<class T>
ErrorCode Worker::MergeImpl(const Slice& key, const T& val,
                            const Slice& tgt_branch, const Slice& ref_branch,
                            Hash* ver) {
  const auto& ref_ver_opt = head_ver_.GetBranch(key, ref_branch);
  if (!ref_ver_opt) {
    LOG(ERROR) << "Branch \"" << ref_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  return Merge(key, val, tgt_branch, *ref_ver_opt, ver);
}

template<class T>
ErrorCode Worker::MergeImpl(const Slice& key, const T& val,
                            const Slice& tgt_branch, const Hash& ref_ver,
                            Hash* ver) {
  const auto& tgt_ver_opt = head_ver_.GetBranch(key, tgt_branch);
  if (!tgt_ver_opt) {
    LOG(ERROR) << "Branch \"" << tgt_branch << "\" for Key \"" << key
               << "\" does not exist!";
    return ErrorCode::kBranchNotExists;
  }
  ErrorCode ec = Write(key, val, *tgt_ver_opt, ref_ver, ver);
  if (ec == ErrorCode::kOK) head_ver_.PutBranch(key, tgt_branch, *ver);
  return ec;
}

template<class T>
ErrorCode Worker::MergeImpl(const Slice& key, const T& val,
                            const Hash& ref_ver1, const Hash& ref_ver2,
                            Hash* ver) {
  return Write(key, val, ref_ver1, ref_ver2, ver);
}

}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_H_
