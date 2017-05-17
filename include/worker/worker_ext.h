// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_WORKER_EXT_H_
#define USTORE_WORKER_WORKER_EXT_H_

#include <vector>
#include "worker/worker.h"

namespace ustore {

class WorkerExt : public Worker {
 public:
  explicit WorkerExt(const WorkerID& id) : Worker(id) {}
  ~WorkerExt() {}

  /**
   * @brief Obtain all the latest versions of data.
   *
   * @param key Data key.
   * @return A set of all the latest versions of data.
   */
  // TODO(linqian): later on, we may have filters on the returned versions, e.g,
  //  return last 10 latest versions
  inline std::vector<Hash> GetLatestVersions(const Slice& key) const {
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
   * @return A set of all the branches of the data.
   */
  inline std::vector<Slice> ListBranch(const Slice& key) const {
    return head_ver_.ListBranch(key);
  }

  inline bool Exists(const Slice& key) const {
    return head_ver_.Exists(key);
  }

  /**
   * @brief Check for the existence of the specified branch.
   * @param key Data key.
   * @param branch The specified branch.
   * @return True if the specified branch exists for the data;
   *         otherwise false.
   */
  inline bool Exists(const Slice& key, const Slice& branch) const {
    return head_ver_.Exists(key, branch);
  }

  /**
   * @brief Check whether the given version is the head version of the
   *        specified branch.
   *
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

  ErrorCode GetForType(const UType& type, const Slice& key,
                       const Hash& ver, UCell* ucell);

  inline ErrorCode GetForType(const UType& type, const Slice& key,
                              const Slice& branch, UCell* ucell) {
    return GetForType(type, key, GetBranchHead(key, branch), ucell);
  }
};

}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_EXT_H_
