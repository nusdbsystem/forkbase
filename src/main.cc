// Copyright (c) 2017 The Ustore Authors.

#include <gflags/gflags.h>
#include <cassert>
#include "utils/env.h"
#include "cluster/chunk_service.h"
#include "cluster/worker_service.h"

DEFINE_string(node_id, "", "ip address of this node (ib0 interface)");
DEFINE_int32(loglevel, ustore::INFO, "logging severity level");

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cout << "Args: --node_id ip:port [--loglevel 0/1/2/3]\n";
    return 0;
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // set the logging severity level
  ustore::SetStderrLogging(FLAGS_loglevel);

  // create worker/chunk service
  ustore::WorkerService ws(FLAGS_node_id, true);
  ustore::ChunkService cs(FLAGS_node_id);

  // Start chunk service if dist store is enabled
  // if (ustore::Env::Instance()->config().enable_dist_store()) cs.Run();
  // start worker and blocking here
  ws.Start();

  return 0;
}

