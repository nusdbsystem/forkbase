// Copyright (c) 2017 The Ustore Authors.

#include "worker/worker.h"
namespace ustore {

ErrorCode Worker::Get(const Slice& key, const Slice& branch,
                      const Hash& version, Value* val) const {  
  return ErrorCode::kOK;
}

ErrorCode Worker::Put(const Slice& key, const Value& val, const Slice& branch,
                      const Hash& previous, Hash* version) {
  return ErrorCode::kOK;
}

ErrorCode Worker::Branch(const Slice& key, const Slice& old_branch,
                         const Hash& version, const Slice& new_branch) {
  return ErrorCode::kOK;
}

ErrorCode Worker::Move(const Slice& key, const Slice& old_branch,
                       const Slice& new_branch) {
  return ErrorCode::kOK;
}

ErrorCode Worker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Slice& ref_branch,
                        const Hash& ref_version, Hash* version) {
  return ErrorCode::kOK;
}

}  // namespace ustore
