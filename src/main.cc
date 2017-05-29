// Copyright (c) 2017 The Ustore Authors.

#include <gflags/gflags.h>
#include <cassert>
#include "utils/env.h"
#include "cluster/worker_service.h"
#include "cluster/remote_client_service.h"

using ustore::WorkerService;
using ustore::RemoteClientService;
using ustore::INFO;

DEFINE_string(node_id, "", "ip address of this node (ib0 interface)");
DEFINE_int32(loglevel, ustore::INFO, "logging severity level");

int main(int argc, char **argv) {
  if (argc < 2) {
    std::cout << "Args: --node_id ip:port [--loglevel 0/1/2/3]\n";
    return 0;
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // either client service or worker must be set

  // set the logging severity level
  ustore::SetStderrLogging(FLAGS_loglevel);

  // start the worker
  WorkerService ws(FLAGS_node_id, "");
  ws.Init();
  ws.Start();

  return 0;
}

