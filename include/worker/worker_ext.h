// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_WORKER_WORKER_EXT_H_
#define USTORE_WORKER_WORKER_EXT_H_

#include "worker/worker.h"

namespace ustore {

class WorkerExt : public Worker {
 public:
  explicit WorkerExt(const WorkerID& id) : Worker(id) {}
  ~WorkerExt() {}

  ErrorCode GetForType(const UType& type, const Slice& key,
                       const Hash& ver, UCell* ucell);

  inline ErrorCode GetForType(const UType& type, const Slice& key,
                              const Slice& branch, UCell* ucell) {
    return GetForType(type, key, GetBranchHead(key, branch), ucell);
  }

 private:
};

}  // namespace ustore

#endif  // USTORE_WORKER_WORKER_EXT_H_
