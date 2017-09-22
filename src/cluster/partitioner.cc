// Copyright (c) 2017 The Ustore Authors

#include <fstream>
#include <stdlib.h>
#include "cluster/partitioner.h"
#include "utils/env.h"

namespace ustore {

Partitioner::Partitioner(const std::string& hostfile, const std::string& self_addr) {
  // load worker file
  std::ifstream fin(hostfile);
  std::string worker_addr;
  for (int id = 0; fin >> worker_addr; ++id) {
    worker_list_.push_back(worker_addr);
    if (worker_addr == self_addr) id_ = id;
  }
  fin.close();
}

int Partitioner::GetWorkerId(const Hash& hash) const {
  uint64_t idx = *reinterpret_cast<const int64_t*>(hash.value() + 9);
  // uint64_t idx = 0;
  return idx % worker_list_.size();
}

}  // namespace ustore
