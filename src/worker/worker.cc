// Copyright (c) 2017 The Ustore Authors.

#include "worker/worker.h"

namespace ustore {

Worker*
Worker::Instance() {
  static auto ins(new Worker(10));

  return ins;
}

Worker::Worker(const WorkerID& id)
  : id_(id),
    head_ver_(HeadVersion::Instance()) {}

Worker::~Worker() {}

ErrorCode
Worker::Get(const Slice& key,
            const Slice& branch,
            const Hash& version,
            Value* val) const {
  return kOK;
}

ErrorCode
Worker::Put(const Slice& key,
            const Value& val,
            const Slice& branch,
            const Hash& version) {
  return kOK;
}

ErrorCode
Worker::Branch(const Slice& key,
               const Slice& old_branch,
               const Hash& version,
               const Slice& new_branch) {
  return kOK;
}

ErrorCode
Worker::Move(const Slice& key,
             const Slice& old_branch,
             const Slice& new_branch) {
  return kOK;
}

ErrorCode
Worker::Merge(const Slice& key,
              const Value& val,
              const Slice& tgt_branch,
              const Slice& ref_branch,
              const Hash& ref_version) {
  return kOK;
}

} // namespace ustore
