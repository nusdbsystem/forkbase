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
  inline ErrorCode Put(const Slice& key, const Value& val, const Slice& branch,
                       Hash* ver) override {
    return Put(key, val, branch, GetBranchHead(key, branch), ver);
  }

  ErrorCode Put(const Slice& key, const Value& val, const Slice& branch) {
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
  inline ErrorCode Put(const Slice& key, const Value& val, const Hash& prev_ver,
                       Hash* ver) override {
    return Write(key, val, prev_ver, Hash::kNull, ver);
  }

  ErrorCode Put(const Slice& key, const Value& val, const Hash& prev_ver) {
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
  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Slice& ref_branch, Hash* ver) override;

  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Slice& ref_branch) {
    static Hash ver;
    return Merge(key, val, tgt_branch, ref_branch, &ver);
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
  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Hash& ref_ver, Hash* ver) override;

  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Hash& ref_ver) {
    static Hash ver;
    return Merge(key, val, tgt_branch, ref_ver, &ver);
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
  ErrorCode Merge(const Slice& key, const Value& val, const Hash& ref_ver1,
                  const Hash& ref_ver2, Hash* ver) override;

  ErrorCode Merge(const Slice& key, const Value& val,
                  const Hash& ref_ver1, const Hash& ref_ver2) {
    static Hash ver;
    return Merge(key, val, ref_ver1, ref_ver2, &ver);
  }

  Chunk GetChunk(const Slice& key, const Hash& ver) override;

  // TODO(linqian): implement methods below
  ErrorCode ListKeys(std::vector<std::string>* versions)
      override { return ErrorCode::kUnknownOp; }
  ErrorCode ListBranches(const Slice& key, std::vector<std::string>* branches)
      override { return ErrorCode::kUnknownOp; }
  ErrorCode Exist(const Slice& key, bool* exist)
      override { return ErrorCode::kUnknownOp; }
  ErrorCode Exist(const Slice& key, const Slice& branch, bool* exist)
      override { return ErrorCode::kUnknownOp; }
  ErrorCode GetBranchHead(const Slice& key, const Slice& branch, Hash* version)
      override { return ErrorCode::kUnknownOp; }
  ErrorCode IsBranchHead(const Slice& key, const Slice& branch,
                         const Hash& version, bool* isHead)
      override { return ErrorCode::kUnknownOp; }
  ErrorCode GetLatestVersions(const Slice& key, std::vector<Hash>* versions)
      override { return ErrorCode::kUnknownOp; }
  ErrorCode IsLatestVersion(const Slice& key, const Hash& version,
                            bool* isLatest)
      override { return ErrorCode::kUnknownOp; }

 protected:
  HeadVersion head_ver_;

 private:
  ErrorCode CreateUCell(const Slice& key, const UType& utype,
                        const Hash& utype_hash, const Hash& prev_ver1,
                        const Hash& prev_ver2, Hash* ver);
  ErrorCode Write(const Slice& key, const Value& val, const Hash& prev_ver1,
                  const Hash& prev_ver2, Hash* ver);
  ErrorCode WriteBlob(const Slice& key, const Value& val, const Hash& prev_ver1,
                      const Hash& prev_ver2, Hash* ver);
  ErrorCode WriteString(const Slice& key, const Value& val,
                        const Hash& prev_ver1, const Hash& prev_ver2,
                        Hash* ver);
  ErrorCode WriteList(const Slice& key, const Value& val,
                      const Hash& prev_ver1, const Hash& prev_ver2,
                      Hash* ver);
  ErrorCode WriteMap(const Slice& key, const Value& val,
                     const Hash& prev_ver1, const Hash& prev_ver2,
                     Hash* ver);
  ErrorCode Put(const Slice& key, const Value& val, const Slice& branch,
                const Hash& prev_ver, Hash* ver);
  inline void UpdateLatestVersion(const UCell& ucell) {
    const auto& prev_ver1 = ucell.preHash();
    const auto& prev_ver2 = ucell.preHash(true);
    const auto& ver = ucell.hash();
    head_ver_.PutLatest(ucell.key(), prev_ver1, prev_ver2, ver);
  }

  const WorkerID id_;
};

}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_H_
