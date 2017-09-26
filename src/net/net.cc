// Copyright (c) 2017 The Ustore Authors.

#include "net/net.h"

#include <set>
#include <vector>
#ifdef USE_RDMA
#include "net/rdma_net.h"
#else
#include "net/zmq_net.h"
#endif  // USE_RDMA

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

namespace net {

Net* CreateServerNetwork(const node_id_t& id, int n_threads) {
#ifdef USE_RDMA
  return new RdmaNet(id, n_threads);
#else
  return new ServerZmqNet(id, n_threads);
#endif
  LOG(FATAL) << "Failed to create network instance";
  return nullptr;
}

Net* CreateClientNetwork(int n_threads) {
#ifdef USE_RDMA
  return new RdmaNet("", n_threads);
#else
  return new ClientZmqNet(n_threads);
#endif
  LOG(FATAL) << "Failed to create network instance";
  return nullptr;
}

}  // namespace net
}  // namespace ustore
