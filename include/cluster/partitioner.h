// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_PARTITIONER_H_
#define USTORE_CLUSTER_PARTITIONER_H_

#include <string>
#include <vector>

#include "hash/hash.h"
#include "spec/slice.h"

namespace ustore {

/*
 * Partitioner is responsible for guiding the destination worker for each data
 * piece.
 */
class Partitioner {
 public:
  explicit Partitioner(const std::string& hostfile, const std::string& self_addr);
  ~Partitioner() = default;

  // get worker id of a specific hash (For Chunked data types)
  int GetWorkerId(const Hash& hash) const;
  // get worker id of a specific key (For UCell data type)
  inline int GetWorkerId(const Slice& key) const {
    return GetWorkerId(Hash::ComputeFrom(key.data(), key.len()));
  }
  // get worker addr of a specific hash (For Chunked data types)
  inline const std::string& GetWorkerAddr(const Hash& hash) const {
    return id2addr(GetWorkerId(hash));
  }
  // get worker addr of a specific key (For UCell data type)
  inline const std::string& GetWorkerAddr(const Slice& key) const {
    return id2addr(GetWorkerId(key));
  }

  // own id
  inline int id() const { return id_; }
  inline const std::vector<string> workerAddrs() const { return worker_list_; }
  inline const std::string& id2addr(int id) const { return worker_list_[id]; }

 private:
  int id_ = -1;
  std::vector<std::string> worker_list_;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_PARTITIONER_H_
