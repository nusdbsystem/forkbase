// Copyright (c) 2017 The Ustore Authors.

#include "net/net.h"

#include <set>
#include <vector>

namespace ustore {

Net::~Net() {
  for (auto val : netmap_)
    delete val.second;
  DLOG(INFO) << "Destroy Net ";
}

void Net::CreateNetContexts(const std::vector<node_id_t>& nodes) {
  std::set<node_id_t> tmp;
  for (size_t i = 0; i < nodes.size(); i++)
    tmp.insert(nodes[i]);
  for (auto val = netmap_.begin(); val != netmap_.end(); val++) {
    if (tmp.find(val->first) == tmp.end())
      delete val->second;
    else
      tmp.erase(val->first);
  }

  for (auto val = tmp.begin(); val != tmp.end(); val++) {
    if (*val != cur_node_)
      CreateNetContext(*val);
  }
}

void Net::DeleteNetContext(NetContext* ctx) { delete ctx; }

}  // namespace ustore
