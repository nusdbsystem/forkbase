// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_KVDB_WRITE_BATCH_H_
#define USTORE_KVDB_WRITE_BATCH_H_

#include <string>
#include <vector>

namespace ustore_kvdb {

class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch();

  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  void Clear();

 private:
  friend class KVDB;
  enum Type { kInsertion = 0, kDeletion = 1 };
  std::vector<std::string> keys_, vals_;
  std::vector<Type> types_;
};

}  // namespace ustore_kvdb

#endif  // USTORE_KVDB_WRITE_BATCH_H_
