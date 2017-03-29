// Copyright (c) 2017 The Ustore Authors.
#ifdef MOCK_TEST
#include <string>
#include "worker/worker.h"
#include "utils/logging.h"
using std::string;
namespace ustore {
const string RANDOM_VAL =
                "I am a spy, a sleeper, a spook, a man of two faces";
const string BASE_VER = "sympathizer";
ErrorCode MockWorker::Get(const Slice& key, const Slice& branch,
                      const Hash& version, Value* val) const {
  // populate random value
  Blob *val_blob = new Blob((const byte_t*)RANDOM_VAL.data(),
                              RANDOM_VAL.length());
  *val = Value(*val_blob);
  return ErrorCode::kOK;
}

ErrorCode MockWorker::Put(const Slice& key, const Value& val,
           const Slice& branch, const Hash& previous, Hash* version) {
  string ver = BASE_VER + std::to_string(count_put_);
  count_put_++;
  Hash *h = new Hash();
  h->Compute((const byte_t*)ver.data(), ver.length());
  *version = *h;
  return ErrorCode::kOK;
}

ErrorCode MockWorker::Branch(const Slice& key, const Slice& old_branch,
                         const Hash& version, const Slice& new_branch) {
  return ErrorCode::kOK;
}

ErrorCode MockWorker::Move(const Slice& key, const Slice& old_branch,
                       const Slice& new_branch) {
  return ErrorCode::kOK;
}

ErrorCode MockWorker::Merge(const Slice& key, const Value& val,
                        const Slice& tgt_branch, const Slice& ref_branch,
                        const Hash& ref_version, Hash* version) {
  string ver = BASE_VER + std::to_string(count_merge_);
  count_merge_++;
  Hash *h = new Hash();
  h->Compute((const byte_t*)ver.data(), ver.length());
  *version = *h;
  return ErrorCode::kOK;
}

}  // namespace ustore
#endif

