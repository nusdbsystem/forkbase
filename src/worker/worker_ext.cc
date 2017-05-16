// Copyright (c) 2017 The Ustore Authors.

#include "utils/utils.h"
#include "worker/worker_ext.h"

namespace ustore {

ErrorCode WorkerExt::GetForType(const UType& type, const Slice& key,
                                const Hash& ver, UCell* ucell) {
  USTORE_GUARD(Get(key, ver, ucell));
  if (ucell->type() != type) {
    LOG(ERROR) << "Type mismatch: [Actual] " << ucell->type()
               << ", [Expected] " << type;
    return ErrorCode::kTypeMismatch;
  }
  return ErrorCode::kOK;
}

}  // namespace ustore
