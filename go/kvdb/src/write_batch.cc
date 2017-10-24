// Copyright (c) 2017 The Ustore Authors.
#include "write_batch.h"

namespace ustore_kvdb {

WriteBatch::WriteBatch() { Clear(); }
WriteBatch::~WriteBatch() {}

void WriteBatch::Put(const std::string& key, const std::string& value) {
  keys_.push_back(key);
  vals_.push_back(value);
  types_.push_back(kInsertion);
}
void WriteBatch::Delete(const std::string& key) {
  keys_.push_back(key);
  types_.push_back(kDeletion);
}
void WriteBatch::Clear() {
  keys_.clear();
  vals_.clear();
  types_.clear();
}

}  // namespace ustore_kvdb
