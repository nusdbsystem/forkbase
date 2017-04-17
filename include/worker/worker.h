// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_WORKER_H_
#define USTORE_WORKER_WORKER_H_

#include <unordered_set>
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "types/ucell.h"
#include "utils/noncopyable.h"
#include "worker/head_version.h"

namespace ustore {

/**
 * @brief Worker node management.
 */
class Worker : private Noncopyable {
 public:
  using WorkerID = uint32_t;
  static const Slice kNullBranch;

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
  const Hash GetBranchHead(const Slice& key, const Slice& branch) const;

  /**
   * @brief Obtain all the latest versions of data.
   *
   * @param key Data key.
   * @return A set of all the latest versions of data.
   */
  inline const std::unordered_set<Hash>& GetLatestVersions(const Slice& key)
      const {
    return head_ver_.GetLatest(key);
  }

  /**
   * @brief Check if the given version is one of the latest versions of data.
   *
   * @param key Data key.
   * @param ver Data version.
   */
  inline bool IsLatest(const Slice& key, const Hash& ver) const {
    return head_ver_.IsLatest(key, ver);
  }

  /**
   * @brief List all the branchs of data.
   *
   * @param key Data key.
   * @return A set of all the branches of data.
   */
  inline std::unordered_set<Slice> ListBranch(const Slice& key) const {
    return head_ver_.ListBranch(key);
  }

  /**
   * @brief Check whether the given version is the head version of the
   *        specified branch.
   * @param key Data key.
   * @param branch The operating branch.
   * @param ver Data version.
   * @return True if the given version is the head version of the specified
   *         branch; otherwise false.
   */
  inline bool IsBranchHead(const Slice& key, const Slice& branch,
                           const Hash& ver) const {
    return head_ver_.IsBranchHead(key, branch, ver);
  }

  /**
   * @brief Read data.
   *
   * @param key Data key.
   * @param branch The operating branch.
   * @param ver Data version.
   * @param val Accommodator of the to-be-retrieved value.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Get(const Slice& key, const Slice& branch,
                        const Hash& ver, Value* val) const;

  /**
   * @brief Read data.
   *
   * @param key Data key.
   * @param branch The operating branch.
   * @param val Accommodator of the to-be-retrieved value.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Get(const Slice& key, const Slice& branch, Value* val)
      const;

  /**
   * @brief Read data.
   *
   * @param key Data key.
   * @param ver Data version.
   * @param val Accommodator of the to-be-retrieved value.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Get(const Slice& key, const Hash& ver, Value* val) const;

  /**
   * @brief Write data.
   *
   * @param key Data key.
   * @param val Data val.
   * @param branch The operating branch.
   * @param prev_ver The previous ver of data.
   * @param ver Accommodator of the new data version.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value& val, const Slice& branch,
                        const Hash& prev_ver, Hash* ver);

  /**
   * @brief Write data.
   *
   * Write data based on the branch head. If the branch does not exist, the
   * write will be based on the Hash::kNull.
   *
   * @param key Data key.
   * @param val Data val.
   * @param branch The operating branch.
   * @param ver Accommodator of the new data version.
   * @return Error code. (0 for success)
   */
  ErrorCode Put(const Slice& key, const Value& val, const Slice& branch,
                Hash* ver);

  /**
   * @brief Write data.
   *
   * @param key Data key.
   * @param val Data val.
   * @param prev_ver The previous ver of data.
   * @param ver Accommodator of the new data version.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Put(const Slice& key, const Value& val,
                        const Hash& prev_ver, Hash* ver);

  /**
   * @brief Create a new branch for the data.
   *
   * @param old_branch The base branch. If provided, the new branch should be
   *                   based on the head of the base branch.
   * @param ver Data version.
   * @param new_branch The new branch.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Slice& old_branch,
                           const Hash& ver, const Slice& new_branch);

  /**
   * @brief Create a new branch for the data.
   *
   * @param old_branch The base branch.
   * @param new_branch The new branch.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch);

  /**
   * @brief Create a new branch for the data.
   *
   * @param ver Data version.
   * @param new_branch The new branch.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Branch(const Slice& key, const Hash& ver,
                           const Slice& new_branch);

  /**
   * @brief Rename the branch.
   *
   * @param key Data key.
   * @param old_branch The original branch.
   * @param new_branch The target branch.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Move(const Slice& key, const Slice& old_branch,
                         const Slice& new_branch);

  /**
   * @brief Merge two branches of the data.
   *
   * @param key Data key.
   * @param val Data value.
   * @param tgt_branch The target branch.
   * @param ref_branch The referring branch.
   * @param ref_ver The referring version of data.
   * @param ver Accommodator of the new data version.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& val,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          const Hash& ref_ver, Hash* ver);

  /**
   * @brief Merge two branches of the data.
   *
   * @param key Data key.
   * @param val Data value.
   * @param tgt_branch The target branch.
   * @param ref_branch The referring branch.
   * @param ver Accommodator of the new data version.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& val,
      const Slice& tgt_branch, const Slice& ref_branch, Hash* ver);

  /**
   * @brief Merge two branches of the data.
   *
   * @param key Data key.
   * @param val Data value.
   * @param tgt_branch The target branch.
   * @param ref_ver The referring version of data.
   * @param ver Accommodator of the new data version.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& val,
      const Slice& tgt_branch, const Hash& ref_ver, Hash* ver);

  /**
   * @brief Merge two versions of the data.
   *
   * @param key Data key.
   * @param val Data value.
   * @param ref_ver1 The referring version of data.
   * @param ref_ver2 The referring version of data.
   * @param ver Accommodator of the new data version.
   * @return Error code. (0 for success)
   */
  virtual ErrorCode Merge(const Slice& key, const Value& val,
      const Hash& ref_ver1, const Hash& ref_ver2, Hash* ver);

 private:
  ErrorCode EitherBranchOrVersion(
    const Slice& branch, std::function<ErrorCode()> f_run_for_branch,
    const Hash& ver, std::function<ErrorCode()> f_run_for_version) const;
  ErrorCode Read(const UCell& ucell, Value* val) const;
  ErrorCode ReadBlob(const UCell& ucell, Value* val) const;
  ErrorCode ReadString(const UCell& ucell, Value* val) const;
  ErrorCode Write(const Slice& key, const Value& val, const Hash& prev_ver1,
                  const Hash& prev_ver2, Hash* ver) const;
  ErrorCode WriteBlob(const Slice& key, const Value& val, const Hash& prev_ver1,
                      const Hash& prev_ver2, Hash* ver) const;
  ErrorCode WriteString(const Slice& key, const Value& val,
                        const Hash& prev_ver1, const Hash& prev_ver2,
                        Hash* ver) const;
  ErrorCode CreateUCell(const Slice& key, const UType& utype,
                        const Hash& utype_hash, const Hash& prev_ver1,
                        const Hash& prev_ver2, Hash* ver) const;

  const WorkerID id_;
  HeadVersion head_ver_;
};

#ifdef MOCK_TEST
class MockWorker : public Worker {
 public:
  explicit MockWorker(const WorkerID& id) : Worker(id), count_put_(0),
    count_merge_(0) {}

  ErrorCode Get(const Slice& key, const Slice& branch, const Hash& ver,
                Value* val) const;
  ErrorCode Get(const Slice& key, const Slice& branch,
                Value* val) const {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }
  ErrorCode Get(const Slice& key, const Hash& version,
                Value* val) const {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }

  ErrorCode Put(const Slice& key, const Value& val, const Slice& branch,
                const Hash& previous, Hash* ver);
  ErrorCode Put(const Slice& key, const Value& val, const Slice& branch,
                Hash* ver) {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }

  ErrorCode Put(const Slice& key, const Value& val, const Hash& version,
                Hash* ver) {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }

  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                   const Hash& ver, const Slice& new_branch);
  ErrorCode Branch(const Slice& key,
                   const Hash& ver, const Slice& new_branch) {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }

  ErrorCode Branch(const Slice& key,
                   const Slice& old_branch, const Slice& new_branch) {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }


  ErrorCode Move(const Slice& key, const Slice& old_branch,
                 const Slice& new_branch);

  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Slice& ref_branch, const Hash& ref_ver,
                  Hash* ver);
  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Slice& ref_branch,
                  Hash* ver) {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }

  ErrorCode Merge(const Slice& key, const Value& val, const Slice& tgt_branch,
                  const Hash& ref_ver,
                  Hash* ver) {
    LOG(FATAL) << "Method not implemented in MockWorker. Use Worker instead!";
    return ErrorCode::kUnknownOp;
  }


 private:
  int count_put_;  // number of requests seen so far
  int count_merge_;
};
#endif
}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_H_
