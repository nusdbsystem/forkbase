// Copyright (c) 2017 The Ustore Authors

#include <fstream>
#include "cluster/partitioner.h"
#include "utils/env.h"

namespace ustore {

Partitioner::Partitioner(const std::string& hostfile,
                         const std::string& self_addr, bool xor_port) {
  // load worker file
  std::ifstream fin(hostfile);
  std::string dest_addr;
  for (int id = 0; fin >> dest_addr; ++id) {
    if (dest_addr == self_addr) id_ = id;
    // chunk service use XOR-ed port
    if (xor_port) dest_addr.back() ^= 1;
    dest_list_.push_back(dest_addr);
  }
  fin.close();
}

}  // namespace ustore
