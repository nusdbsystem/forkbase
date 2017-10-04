// Copyright (c) 2017 The Ustore Authors

#include <fstream>
#include "cluster/partitioner.h"
#include "utils/env.h"

namespace ustore {

Partitioner::Partitioner(const std::string& hostfile,
                         const std::string& self_addr,
                         std::function<std::string(std::string)> f_port) {
  // load worker file
  std::ifstream fin(hostfile);
  std::string dest_addr;
  for (int id = 0; fin >> dest_addr; ++id) {
    if (dest_addr == self_addr) id_ = id;
    dest_list_.push_back(f_port(dest_addr));
  }
  fin.close();
  CHECK(dest_list_.size()) << "IP:PORT list cannot be empty";
}

}  // namespace ustore
